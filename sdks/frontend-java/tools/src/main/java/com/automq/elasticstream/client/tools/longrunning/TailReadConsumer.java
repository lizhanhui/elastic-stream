package com.automq.elasticstream.client.tools.longrunning;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;

public class TailReadConsumer implements Runnable {

    private static Logger log = Logger.getLogger(TailReadConsumer.class.getClass());
    private Stream stream;
    private long startSeq;
    private long nextSeq;
    private long nextOffset;
    private AtomicLong endOffset;
    private Producer producer;

    public TailReadConsumer(Stream stream, long startSeq, AtomicLong endOffset,
            Producer producer) {
        this.stream = stream;
        this.startSeq = startSeq;
        this.nextSeq = startSeq;
        this.nextOffset = 0;
        this.endOffset = endOffset;
        this.producer = producer;
    }

    @Override
    public void run() {
        log.info("TailReadConsumerThread thread started");
        while (true) {
            if (this.producer.isTerminated()) {
                return;
            }
            try {
                long endOffset = this.endOffset.get();
                log.info("StreamID: " + stream.streamId() + ", tail read, nextSeq: " + this.nextSeq
                        + ", nextOffset: " + nextOffset + ", endOffset: "
                        + endOffset);
                FetchResult fetchResult = this.stream.fetch(this.nextOffset, endOffset,
                        Integer.MAX_VALUE).get();
                List<RecordBatchWithContext> recordBatch = fetchResult.recordBatchList();
                log.info("StreamID: " + stream.streamId() + ", tail read, fetch a recordBatch, size: "
                        + recordBatch.size());
                for (int i = 0; i < recordBatch.size(); i++) {
                    RecordBatchWithContext record = recordBatch.get(i);
                    byte[] rawPayload = new byte[record.rawPayload().remaining()];
                    record.rawPayload().get(rawPayload);
                    if (Utils.checkRecord(this.nextSeq, ByteBuffer.wrap(rawPayload)) == false) {
                        log.error("StreamID: " + stream.streamId() + ", tail read, something wrong with record[" + i
                                + "]");
                        this.producer.terminate();
                    } else {
                        this.nextSeq++;
                    }
                }
                this.nextOffset = this.nextOffset + recordBatch.size();
                fetchResult.free();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString());
                this.producer.terminate();
            }
        }
    }
}
