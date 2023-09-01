package com.automq.elasticstream.client.tools.e2e;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;

public class VerifiableConsumer {
    public static void main(String[] args) {
        E2EOption option = new E2EOption();
        Client client = Client.builder().endpoint(option.getEndPoint()).kvEndpoint(option.getKvEndPoint()).build();
        Stream stream;
        try {
            stream = client.streamClient()
                    .openStream(option.getStreamId(), OpenStreamOptions.newBuilder().build()).get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Open error: " + e.toString());
            return;
        }
        for (long i = option.getStartSeq(); i < option.getCount(); i++) {
            try {
                FetchResult fetchResult = stream.fetch(i, i + 1, Integer.MAX_VALUE).get();
                List<RecordBatchWithContext> recordBatch = fetchResult.recordBatchList();
                if (recordBatch.size() != 1) {
                    System.out.println("Fetch error: wrong size");
                    return;
                }
                RecordBatchWithContext record = recordBatch.get(0);
                byte[] rawPayload = new byte[record.rawPayload().remaining()];
                record.rawPayload().get(rawPayload);
                if (Utils.checkRecord(i, ByteBuffer.wrap(rawPayload)) == false) {
                    System.out.println("Fetch error: corrupted");
                    return;
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Fetch error: " + e.toString());
                return;
            }
        }
        System.out.println("Fetch complete");
    }
}
