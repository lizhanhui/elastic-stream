package com.automq.elasticstream.client.examples.longrunning;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;
import com.automq.elasticstream.client.DefaultRecordBatch;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;
import java.util.Random;

public class LongRunning {

    private static Logger log = Logger.getLogger(LongRunning.class.getClass());

    public static void main(String[] args) throws Exception {
        LongRunningOption option = new LongRunningOption();
        log.info("EndPoint: " + option.getEndPoint() + ", KvEndPoint: " + option.getKvEndPoint()
                + ", ReplicaCount: " + option.getReplicaCount() + ", AppendInterval: " + option.getInterval()
                + ", PayloadSizeMin: "
                + option.getMin() + ", PayloadSizeMax: " + option.getMax());
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint()).build();
        Stream stream = client.streamClient()
                .createAndOpenStream(
                        CreateStreamOptions.newBuilder().replicaCount(option.getReplicaCount()).build())
                .get();
        long streamId = stream.streamId();
        log.info("Created Stream, StreamID: " + streamId);
        BlockingQueue<Elem> crcQueue = new LinkedBlockingQueue<>(1024);
        Thread producerThread = new Thread(
                new Producer(crcQueue, stream, option.getInterval(), option.getMin(), option.getMax()));
        Thread consumerThread = new Thread(new Consumer(crcQueue, stream));
        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();
        stream.close().get();
    }
}

class Elem {
    long crc;
    long offset;

    Elem(long crc, long offset) {
        this.crc = crc;
        this.offset = offset;
    }

    long getCrc() {
        return this.crc;
    }

    long getOffset() {
        return this.offset;
    }
}

class LongRunningOption {
    private String endpoint = "127.0.0.1:12378";
    private String kvEndpoint = "127.0.0.1:12379";
    private int replicaCount = 1;
    private long appendInterval = 100;
    private int payloadSizeMin = 1024;
    private int payloadSizeMax = 4096;

    public LongRunningOption() {
        String endpoint = System.getenv("END_POINT");
        if (endpoint != null) {
            this.endpoint = endpoint;
        }
        String kvEndpoint = System.getenv("KV_END_POINT");
        if (kvEndpoint != null) {
            this.kvEndpoint = kvEndpoint;
        }
        String replicaCountStr = System.getenv("REPLICA_COUNT");
        if (replicaCountStr != null) {
            this.replicaCount = Integer.parseInt(replicaCountStr);
        }
        String intervalStr = System.getenv("APPEND_INTERVAL");
        if (intervalStr != null) {
            this.appendInterval = Long.parseLong(intervalStr);
        }
        String minStr = System.getenv("PAYLOAD_SIZE_MIN");
        if (minStr != null) {
            this.payloadSizeMin = Integer.parseInt(minStr);
        }
        String maxStr = System.getenv("PAYLOAD_SIZE_MAX");
        if (maxStr != null) {
            this.payloadSizeMax = Integer.parseInt(maxStr);
        }
    }

    public String getEndPoint() {
        return this.endpoint;
    }

    public String getKvEndPoint() {
        return this.kvEndpoint;
    }

    public int getReplicaCount() {
        return this.replicaCount;
    }

    public long getInterval() {
        return this.appendInterval;
    }

    public int getMin() {
        return this.payloadSizeMin;
    }

    public int getMax() {
        return this.payloadSizeMax;
    }
}

class Utils {
    public static byte[] generateRandomByteArray(int min, int max) {
        int length = getRandomLength(min, max);
        byte[] byteArray = new byte[length];
        new Random().nextBytes(byteArray);
        return byteArray;
    }

    public static int getRandomLength(int min, int max) {
        Random random = new Random();
        return min + random.nextInt(max - min);
    }

    public static long calculateCRC32(byte[] byteArray) {
        CRC32 crc32 = new CRC32();
        crc32.update(byteArray);
        return crc32.getValue();
    }
}

class Producer implements Runnable {
    private final BlockingQueue<Elem> crcQueue;
    private Stream stream;
    private long interval;
    private int min;
    private int max;
    private static Logger log = Logger.getLogger(Producer.class.getClass());

    public Producer(BlockingQueue<Elem> crcQueue, Stream stream, long interval, int min, int max) {
        this.crcQueue = crcQueue;
        this.stream = stream;
        this.interval = interval;
        this.min = min;
        this.max = max;
    }

    @Override
    public void run() {
        log.info("Producer thread started");
        while (true) {
            try {
                byte[] payload = Utils.generateRandomByteArray(this.min, this.max);
                long crc32 = Utils.calculateCRC32(payload);
                log.info("crc32: " + crc32);
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                CompletableFuture<AppendResult> cf = stream
                        .append(new DefaultRecordBatch(10, 0, Collections.emptyMap(), buffer));
                cf.whenComplete((rst, ex) -> {
                    if (ex == null) {
                        long offset = rst.baseOffset();
                        try {
                            crcQueue.put(new Elem(crc32, offset));
                        } catch (InterruptedException e) {
                            log.error(e.toString());
                            return;
                        }
                        log.info("Append a record batch, offset: " + offset);
                    } else {
                        log.error(ex.toString());
                    }
                });
                Thread.sleep(this.interval);
            } catch (InterruptedException e) {
                log.error(e.toString());
                continue;
            }
        }
    }
}

class Consumer implements Runnable {
    private final BlockingQueue<Elem> crcQueue;
    private Stream stream;

    private static Logger log = Logger.getLogger(Consumer.class.getClass());

    public Consumer(BlockingQueue<Elem> crcQueue, Stream stream) {
        this.crcQueue = crcQueue;
        this.stream = stream;
    }

    @Override
    public void run() {
        log.info("Consumer thread started");
        while (true) {
            try {
                Elem elem = crcQueue.take();
                FetchResult fetchResult = stream.fetch(elem.getOffset(), elem.getOffset() +
                        10, Integer.MAX_VALUE)
                        .get();
                RecordBatchWithContext recordBatch = fetchResult.recordBatchList().get(0);
                byte[] rawPayload = new byte[recordBatch.rawPayload().remaining()];
                recordBatch.rawPayload().get(rawPayload);
                long crc0 = elem.getCrc();
                long crc = Utils.calculateCRC32(rawPayload);
                if (crc != crc0) {
                    log.error("Fetch error, offset: " + elem.getOffset() + ", crc: " + crc + ", crc0: " + crc0);
                    continue;
                }
                log.info("Fetch a record batch, offset: " + elem.getOffset());
                fetchResult.free();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.toString());
                continue;
            }
        }
    }
}
