package com.automq.elasticstream.client.examples.longrunning;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class LongRunning {
    private static Logger log = Logger.getLogger(LongRunning.class.getClass());

    public static void main(String[] args) throws Exception {
        LongRunningOption option = new LongRunningOption();
        log.info("EndPoint: " + option.getEndPoint() + ", KvEndPoint: " +
                option.getKvEndPoint()
                + ", AppendInterval: " +
                option.getInterval()
                + ", PayloadSizeMin: "
                + option.getMin() + ", PayloadSizeMax: " + option.getMax());
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint())
                .build();
        ExecutorService executor = Executors.newFixedThreadPool(9);
        for (int replica = 1; replica <= 3; replica++) {
            Stream stream = client.streamClient()
                    .createAndOpenStream(
                            CreateStreamOptions.newBuilder().replicaCount(replica).build())
                    .get();
            createTask(stream, Utils.getRandomInt(0, 1024), option.getMin(),
                    option.getMax(), option.getInterval(), executor);
        }
    }

    private static void createTask(Stream stream,
            int startSeq,
            int minSize,
            int maxSize,
            long interval, ExecutorService executor) {
        AtomicLong endOffset = new AtomicLong(0);
        executor.submit(new Producer(stream, startSeq, minSize, maxSize, interval, endOffset));
        executor.submit(new TailReadConsumer(stream,
                startSeq, endOffset));
        executor.submit(new RepeatedReadConsumer(stream, startSeq, endOffset));
    }
}
