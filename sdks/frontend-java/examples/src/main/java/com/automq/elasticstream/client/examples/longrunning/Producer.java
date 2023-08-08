package com.automq.elasticstream.client.examples.longrunning;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.DefaultRecordBatch;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.Stream;

public class Producer implements Runnable {
    private static Logger log = Logger.getLogger(Producer.class.getClass());
    private Stream stream;
    private long startSeq;
    private int minSize;
    private int maxSize;
    private long interval;
    private long nextSeq;
    private long lastSeq;
    private ByteBuffer lastRecord;
    private AtomicLong endOffset;

    public Producer(Stream stream, long startSeq, int minSize, int maxSize, long interval,
            AtomicLong endOffset) {
        this.stream = stream;
        this.startSeq = startSeq;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.nextSeq = startSeq;
        this.interval = interval;
        this.lastSeq = -1;
        this.endOffset = endOffset;
    }

    @Override
    public void run() {
        log.info("Producer thread started");
        while (true) {
            try {
                ByteBuffer buffer;
                if (this.lastSeq != this.nextSeq) {
                    buffer = Utils.getRecord(this.nextSeq, this.minSize, this.maxSize);
                    this.lastRecord = buffer;
                    this.lastSeq = this.nextSeq;
                } else {
                    log.info("Retry append...");
                    buffer = this.lastRecord;
                }
                AppendResult result = this.stream
                        .append(new DefaultRecordBatch(1, 0, Collections.emptyMap(), buffer)).get();
                log.info("Append a record batch, seq: " + this.nextSeq + ", offset: " + result.baseOffset());
                this.endOffset.set(result.baseOffset() + 1);
                this.nextSeq++;
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString());
            }
        }
    }
}
