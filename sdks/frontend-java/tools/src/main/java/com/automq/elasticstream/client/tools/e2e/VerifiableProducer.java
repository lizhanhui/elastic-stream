package com.automq.elasticstream.client.tools.e2e;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import com.automq.elasticstream.client.DefaultRecordBatch;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.Stream;

public class VerifiableProducer {
    public static void main(String[] args) {
        E2EOption option = new E2EOption();
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint()).build();
        Stream stream;
        try {
            if (option.getStreamId() >= 0) {
                stream = client.streamClient()
                        .openStream(option.getStreamId(), OpenStreamOptions.newBuilder().build()).get();

            } else {
                stream = client.streamClient()
                        .createAndOpenStream(CreateStreamOptions.newBuilder().replicaCount(option.getReplica()).build())
                        .get();
            }
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Open error: " + e.toString());
            return;
        }
        for (long i = option.getStartSeq(); i < option.getCount(); i++) {
            ByteBuffer buffer = Utils.getRecord(i);
            try {
                AppendResult result = stream
                        .append(new DefaultRecordBatch(1, 0, Collections.emptyMap(),
                                buffer))
                        .get();
                // System.out.println("offset: " + result.baseOffset());
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Append error: " + e.toString());
                return;
            }
        }
        System.out.println("Append complete");
    }
}
