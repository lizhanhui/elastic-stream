package com.automq.elasticstream.client.tools.longrunning;

import org.apache.log4j.Logger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class LongRunning {
    private static Logger log = Logger.getLogger(LongRunning.class.getClass());
    // 10s
    private static final long INTERVAL = 10 * 1000;

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
        Thread[] array = new Thread[3];
        for (int replica = 1; replica <= 3; replica++) {
            int finalReplica = replica;
            array[replica - 1] = new Thread(() -> {
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
                        TailReadConsumer tailReadConsumer = new TailReadConsumer(stream, startSeq, endOffset,
                                producer);
                        RepeatedReadConsumer repeatedReadConsumer = new RepeatedReadConsumer(stream, startSeq,
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
        for (int i = 0; i < 3; i++) {
            array[i].start();
        }
        for (int i = 0; i < 3; i++) {
            try {
                array[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
