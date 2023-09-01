package com.automq.elasticstream.client.tools.e2e;

import static org.junit.Assert.assertEquals;
import java.util.concurrent.ExecutionException;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class MetadataTest {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        E2EOption option = new E2EOption();
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint()).build();

        assertEquals(Utils.openStream(client, 0, OpenStreamOptions.newBuilder().build()), null);
        assertEquals(Utils.openStream(client, Long.MAX_VALUE, OpenStreamOptions.newBuilder().build()), null);

        // 1. Create stream and check its stream id
        for (int i = 0; i < option.getCount(); i++) {
            Stream stream = client.streamClient()
                    .createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(option.getReplica()).build())
                    .get();
            assertEquals(stream.streamId(), i);
            stream.close();
        }
        // 2. Reopen stream and check its stream id
        for (long i = 0; i < option.getCount(); i++) {
            Stream stream = client.streamClient().openStream(i, OpenStreamOptions.newBuilder().build()).get();
            assertEquals(stream.streamId(), i);
            stream.close();
        }
        System.out.println("PASS");
    }
}
