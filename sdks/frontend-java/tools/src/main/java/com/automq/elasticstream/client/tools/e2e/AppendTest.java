package com.automq.elasticstream.client.tools.e2e;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;

import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class AppendTest {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        E2EOption option = new E2EOption();
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint()).build();
        // 1. Create an new stream, append records to it and fetch records from it
        Stream stream0 = client.streamClient()
                .createAndOpenStream(CreateStreamOptions.newBuilder().epoch(0)
                        .replicaCount(option.getReplica()).build())
                .get();
        long streamId = stream0.streamId();
        assertTrue(Utils.appendRecords(stream0, 0, option.getCount(), option.getBatchSize()));
        assertTrue(Utils.fetchRecords(stream0, 0, option.getCount(), option.getBatchSize()));
        stream0.close().get();
        // 2 . Open a stream with new epoch, append records to it and fetch records
        // from it
        Stream stream1 = Utils.openStream(client, streamId, OpenStreamOptions.newBuilder().epoch(1).build());
        assertTrue(stream1 != null);
        assertTrue(Utils.appendRecords(stream1, option.getCount(), option.getCount(), option.getBatchSize()));
        assertTrue(Utils.fetchRecords(stream1, option.getCount(), option.getCount(), option.getBatchSize()));
        stream1.close().get();
        // 3 . Open a stream with old epoch
        Stream stream2 = Utils.openStream(client, streamId, OpenStreamOptions.newBuilder().epoch(0).build());
        assertTrue(stream2 == null);
        System.out.println("PASS");
    }
}
