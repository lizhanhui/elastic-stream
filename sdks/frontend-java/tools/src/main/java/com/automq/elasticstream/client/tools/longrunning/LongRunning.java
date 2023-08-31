package com.automq.elasticstream.client.tools.longrunning;

import org.apache.log4j.Logger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class LongRunning {
    private static Logger log = Logger.getLogger(LongRunning.class.getClass());
    // 10s
    private static final long INTERVAL = 10 * 1000;
    private static final long RECORDS_COUNT = 64;

    public static void main(String[] args) {
        LongRunningOption option = new LongRunningOption();
        log.info("EndPoint: " + option.getEndPoint() + ", KvEndPoint: " +
                option.getKvEndPoint()
                + ", AppendInterval: " +
                option.getInterval()
                + ", PayloadSizeMin: "
                + option.getMin() + ", PayloadSizeMax: " + option.getMax());
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint())
                .build();
        Thread[] appendFetchThreads = new Thread[3];
        Thread[] metadataThreads = new Thread[3];
        // Append and fetch task
        for (int replica = 1; replica <= 3; replica++) {
            int finalReplica = replica;
            appendFetchThreads[replica - 1] = new Thread(() -> {
                while (true) {
                    try {
                        Stream stream = client.streamClient()
                                .createAndOpenStream(CreateStreamOptions.newBuilder()
                                        .replicaCount(finalReplica).build())
                                .get();
                        AtomicLong endOffset = new AtomicLong(0);
                        long startSeq = Utils.getRandomInt(0, 1024);
                        Producer producer = new Producer(stream, startSeq,
                                option.getMin(), option.getMax(), option.getInterval(),
                                endOffset, finalReplica);
                        TailReadConsumer tailReadConsumer = new TailReadConsumer(stream, startSeq,
                                endOffset,
                                producer);
                        RepeatedReadConsumer repeatedReadConsumer = new RepeatedReadConsumer(stream,
                                startSeq,
                                endOffset, producer);
                        Thread producerThread = new Thread(producer);
                        producerThread.start();
                        Thread tailReadConsumerThread = new Thread(tailReadConsumer);
                        tailReadConsumerThread.start();
                        Thread repeatedReadConsumerThread = new Thread(repeatedReadConsumer);
                        repeatedReadConsumerThread.start();

                        producerThread.join();
                        tailReadConsumerThread.join();
                        repeatedReadConsumerThread.join();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Fail to create stream: " + e.toString());
                    }
                    log.info("Restarting task...");
                    try {
                        Thread.sleep(INTERVAL);
                    } catch (InterruptedException e) {
                    }
                }
            });
        }
        // Metadata task
        for (int replica = 1; replica <= 3; replica++) {
            int finalReplica = replica;
            metadataThreads[replica - 1] = new Thread(() -> {
                while (true) {
                    try {
                        Stream stream0 = client.streamClient()
                                .createAndOpenStream(CreateStreamOptions.newBuilder()
                                        .replicaCount(finalReplica).build())
                                .get();
                        long streamId = stream0.streamId();
                        // 1. Create an new stream
                        log.info("1. Create and open stream[" + streamId + "], replica count: " + finalReplica);
                        if (!Utils.appendRecords(stream0, 0, RECORDS_COUNT, 1)) {
                            log.error("Failed to append records to stream, replica count: " + finalReplica);
                            continue;
                        }
                        if (!Utils.fetchRecords(stream0, 0, RECORDS_COUNT, 1)) {
                            log.error("Failed to fetch records from stream, replica count: " + finalReplica);
                            continue;
                        }
                        // 2. Trim stream
                        log.info("2. Trim stream[" + streamId + "]");
                        stream0.trim(RECORDS_COUNT / 2).get();
                        if (Utils.fetchRecords(stream0, 0, RECORDS_COUNT / 2, 1)) {
                            log.error("Fetched records from stream that has been trimmed, replica count: "
                                    + finalReplica);
                            continue;
                        }
                        if (!Utils.fetchRecords(stream0, RECORDS_COUNT / 2, RECORDS_COUNT / 2, 1)) {
                            log.error("Failed to fetch records from stream, replica count: " + finalReplica);
                            continue;
                        }
                        stream0.close().get();
                        // 3. Open stream with new epoch
                        log.info("3. Open stream[" + streamId + "] with epoch: 1");
                        Stream stream1 = Utils.openStream(client, streamId,
                                OpenStreamOptions.newBuilder().epoch(1).build());
                        if (stream1 == null) {
                            log.error("Failed to open stream with new epoch, replica count: " + finalReplica);
                        }
                        if (!Utils.appendRecords(stream1, RECORDS_COUNT, RECORDS_COUNT, 1)) {
                            log.error("Failed to append records to stream, replica count: " + finalReplica);
                            continue;
                        }
                        if (!Utils.fetchRecords(stream1, RECORDS_COUNT, RECORDS_COUNT, 1)) {
                            log.error("Failed to fetch records from stream, replica count: " + finalReplica);
                            continue;
                        }
                        stream1.close().get();
                        // 4. Open stream with old epoch
                        log.info("4. Open stream[" + streamId + "] with epoch: 0");
                        Stream stream2 = Utils.openStream(client, streamId,
                                OpenStreamOptions.newBuilder().epoch(0).build());
                        if (stream2 != null) {
                            log.error("Opened stream with old epoch, replica count: " + finalReplica);
                            continue;
                        }
                        // 5. Destory stream
                        log.info("5. Destroy stream[" + streamId + "]");
                        Stream stream3 = Utils.openStream(client, streamId,
                                OpenStreamOptions.newBuilder().epoch(2).build());
                        if (!Utils.appendRecords(stream3, RECORDS_COUNT + RECORDS_COUNT, RECORDS_COUNT, 1)) {
                            log.error("Failed to append records to stream, replica count: " + finalReplica);
                            continue;
                        }
                        if (!Utils.fetchRecords(stream3, RECORDS_COUNT + RECORDS_COUNT, RECORDS_COUNT, 1)) {
                            log.error("Failed to fetch records from stream, replica count: " + finalReplica);
                            continue;
                        }
                        stream3.destroy().get();
                        if (Utils.appendRecords(stream3, RECORDS_COUNT + RECORDS_COUNT + RECORDS_COUNT, RECORDS_COUNT,
                                1)) {
                            log.error("Appended records to stream that has been destroyed, replica count: "
                                    + finalReplica);
                            continue;
                        }
                        if (Utils.fetchRecords(stream3, RECORDS_COUNT + RECORDS_COUNT + RECORDS_COUNT, RECORDS_COUNT,
                                1)) {
                            log.error("Fetched records from stream that has been destroyed, replica count: "
                                    + finalReplica);
                            continue;
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        log.error(e.toString());
                    }
                    log.info("Restarting metadata task...");
                }
            });
        }
        for (int i = 0; i < 3; i++) {
            appendFetchThreads[i].start();
        }
        for (int i = 0; i < 3; i++) {
            metadataThreads[i].start();
        }
        for (int i = 0; i < 3; i++) {
            try {
                appendFetchThreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < 3; i++) {
            try {
                metadataThreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
