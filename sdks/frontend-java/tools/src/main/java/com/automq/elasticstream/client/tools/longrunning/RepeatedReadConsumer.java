package com.automq.elasticstream.client.tools.longrunning;

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
    private Producer producer;

    public RepeatedReadConsumer(Stream stream, long startSeq, AtomicLong endOffset, Producer producer) {
        this.stream = stream;
        this.startSeq = startSeq;
        this.endOffset = endOffset;
        this.producer = producer;
    }

    @Override
    public void run() {
        log.info("RepeatedReadConsumer thread started");
        while (true) {
            if (this.producer.isTerminated()) {
                return;
            }
            try {
                long endOffset = this.endOffset.get();
                long startOffset = this.stream.startOffset();
                if (endOffset - startOffset >= 1024 * 1024) {
                    startOffset = endOffset - 1024 * 1024;
                }
                log.info("Stream[" + stream.streamId() + "] repeated read, startOffset: " + startOffset
                        + ", endOffset: "
                        + endOffset);
                FetchResult fetchResult = this.stream.fetch(startOffset, endOffset, Integer.MAX_VALUE).get();
                List<RecordBatchWithContext> recordBatch = fetchResult.recordBatchList();
                log.info("Stream[" + stream.streamId() + "] repeated read, fetch a recordBatch, size: "
                        + recordBatch.size());
                long nextSeq = this.startSeq + startOffset;
                for (int i = 0; i < recordBatch.size(); i++) {
                    RecordBatchWithContext record = recordBatch.get(i);
                    byte[] rawPayload = new byte[record.rawPayload().remaining()];
                    record.rawPayload().get(rawPayload);
                    if (Utils.checkRecord(nextSeq, ByteBuffer.wrap(rawPayload)) == false) {
                        log.error("Stream[" + stream.streamId() + "] repeated read, something wrong with record[" + i
                                + "]");
                        this.producer.terminate();
                    } else {
                        nextSeq++;
                    }
                }
                fetchResult.free();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString());
                this.producer.terminate();
            }
        }
    }
}
