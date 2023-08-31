package com.automq.elasticstream.client.tools.longrunning;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
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
    private AtomicLong endOffset;
    private long previousTimestamp;
    private volatile boolean shouldTerminate = false;
    private int replica = 0;

    // 10s
    public static final long TIME_OUT = 1000 * 10;

    public Producer(Stream stream, long startSeq, int minSize, int maxSize, long interval,
            AtomicLong endOffset, int replica) {
        this.stream = stream;
        this.startSeq = startSeq;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.nextSeq = startSeq;
        this.interval = interval;
        this.endOffset = endOffset;
        this.replica = replica;
    }

    @Override
    public void run() {
        log.info("Producer thread started");
        HashSet<Long> confirmOffset = new HashSet<Long>();
        this.previousTimestamp = System.currentTimeMillis();
        HashSet<CompletableFuture<AppendResult>> futureSet = new HashSet<CompletableFuture<AppendResult>>();
        while (true) {
            if (isTerminated()) {
                for (CompletableFuture<AppendResult> element : futureSet) {
                    element.cancel(true);
                }
                return;
            }
            ByteBuffer buffer = Utils.getRecord(this.nextSeq, this.minSize, this.maxSize);
            CompletableFuture<AppendResult> cf = this.stream
                    .append(new DefaultRecordBatch(1, 0, Collections.emptyMap(),
                            buffer));
            futureSet.add(cf);
            log.info("Stream[" + stream.streamId() + ":" + this.replica + "] send a append request, seq: "
                    + this.nextSeq);
            cf.whenComplete((result, e) -> {
                if (isTerminated()) {
                    return;
                }
                if (e == null) {
                    futureSet.remove(cf);
                    long baseOffset = result.baseOffset();
                    confirmOffset.add(baseOffset + 1);
                    log.info("Stream[" + stream.streamId() + ":" + this.replica + "] append a record batch, seq: " +
                            this.nextSeq
                            + ", offset:["
                            + baseOffset + ", " + (baseOffset + 1) + ")");
                } else {
                    log.error(e);
                    futureSet.remove(cf);
                    terminate();
                }
            });
            updateEndOffset(confirmOffset);
            long currentTimestamp = System.currentTimeMillis();
            long elapsedTime = currentTimestamp - previousTimestamp;
            if (elapsedTime > TIME_OUT) {
                log.error("Append timeout, replica count: " + this.replica);
                terminate();
            }
            try {
                Thread.sleep(this.interval);
            } catch (InterruptedException e1) {
                log.error(e1.toString());
            }
            this.nextSeq++;
        }
    }

    void updateEndOffset(HashSet<Long> confirmOffset) {
        long offset = this.endOffset.get();
        while (confirmOffset.contains(offset + 1)) {
            confirmOffset.remove(offset + 1);
            offset++;
        }
        if (offset > this.endOffset.get()) {
            this.previousTimestamp = System.currentTimeMillis();
            this.endOffset.set(offset);
        }
    }

    public void terminate() {
        shouldTerminate = true;
    }

    public boolean isTerminated() {
        return shouldTerminate;
    }
}
