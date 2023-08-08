package com.automq.elasticstream.client.examples.longrunning;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;

public class RepeatedReadConsumer implements Runnable {

    private static Logger log = Logger.getLogger(RepeatedReadConsumer.class.getClass());
    private Stream stream;
    private long startSeq;
    private AtomicLong endOffset;

    public RepeatedReadConsumer(Stream stream, long startSeq, AtomicLong endOffset) {
        this.stream = stream;
        this.startSeq = startSeq;
        this.endOffset = endOffset;
    }

    @Override
    public void run() {
        log.info("RepeatedReadConsumer thread started");
        while (true) {
            try {
                long endOffset = this.endOffset.get();
                long startOffset = this.stream.startOffset();
                log.info("Repeated read, startOffset: " + startOffset + ", endOffset: "
                        + endOffset);
                FetchResult fetchResult = this.stream.fetch(startOffset, endOffset, Integer.MAX_VALUE).get();
                List<RecordBatchWithContext> recordBatch = fetchResult.recordBatchList();
                log.info("Repeated read, fetch a recordBatch, size: " + recordBatch.size());
                long nextSeq = this.startSeq;
                for (int i = 0; i < recordBatch.size(); i++) {
                    RecordBatchWithContext record = recordBatch.get(i);
                    byte[] rawPayload = new byte[record.rawPayload().remaining()];
                    record.rawPayload().get(rawPayload);
                    if (Utils.checkRecord(nextSeq, ByteBuffer.wrap(rawPayload)) == false) {
                        log.error("Repeated read, something wrong with record[" + i + "]");
                    } else {
                        nextSeq++;
                    }
                }
                fetchResult.free();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString());
                continue;
            }
        }
    }
}
