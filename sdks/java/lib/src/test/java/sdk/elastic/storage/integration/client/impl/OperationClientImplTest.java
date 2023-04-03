package sdk.elastic.storage.integration.client.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sdk.elastic.storage.apis.ClientConfigurationBuilder;
import sdk.elastic.storage.apis.OperationClient;
import sdk.elastic.storage.client.impl.OperationClientBuilderImpl;
import sdk.elastic.storage.client.route.Address;
import sdk.elastic.storage.flatc.header.AppendResultT;
import sdk.elastic.storage.flatc.header.CreateStreamResultT;
import sdk.elastic.storage.flatc.header.ErrorCode;
import sdk.elastic.storage.flatc.header.StreamT;
import sdk.elastic.storage.models.RecordBatch;
import sdk.elastic.storage.models.RecordBatchesGenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationClientImplTest {
    private static final Address defaultPmAddress = Address.fromAddress(System.getProperty("pm.address"));
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(8);
    private static OperationClient operationClient;

    @BeforeAll
    public static void setUp() throws Exception {
        ClientConfigurationBuilder builder = new ClientConfigurationBuilder()
            .setPmEndpoint(String.format("%s:%d", defaultPmAddress.getHost(), defaultPmAddress.getPort()))
            .setConnectionTimeout(Duration.ofSeconds(3))
            .setChannelMaxIdleTime(Duration.ofSeconds(10))
            .setHeartBeatInterval(Duration.ofSeconds(9));
        operationClient = new OperationClientBuilderImpl().setClientConfiguration(builder.build()).build();
        operationClient.start();
    }

    @Test
    void testAppendAndFetchBatch() throws ExecutionException, InterruptedException {
        StreamT resultStream = createOneStream().getStream();
        long streamId = resultStream.getStreamId();

        Long startOffset = operationClient.getLastWritableOffset(streamId, DEFAULT_REQUEST_TIMEOUT).get();
        assertEquals(startOffset, 0L);

        RecordBatch batch = RecordBatchesGenerator.generateOneRecordBatch(streamId);
        AppendResultT appendResultT = operationClient.appendBatch(batch, DEFAULT_REQUEST_TIMEOUT).get();

        assertEquals(appendResultT.getStatus().getCode(), ErrorCode.OK);
        assertEquals(appendResultT.getStreamId(), streamId);
        long baseOffset = appendResultT.getBaseOffset();

        List<RecordBatch> recordBatches = operationClient.fetchBatches(streamId, baseOffset, 1, Integer.MAX_VALUE, DEFAULT_REQUEST_TIMEOUT).get();
        assertEquals(1L, recordBatches.size());
        assertTrue(batch.recordsEquivalent(recordBatches.get(0)));

    }

    @Test
    void heartbeat() throws ExecutionException, InterruptedException {
        Boolean result = operationClient.heartbeat(defaultPmAddress, DEFAULT_REQUEST_TIMEOUT).get();
        assertTrue(result);
    }

    private CreateStreamResultT createOneStream() throws ExecutionException, InterruptedException {
        StreamT streamT = new StreamT();
        streamT.setStreamId(0L);
        streamT.setReplicaNums((byte) 1);
        streamT.setRetentionPeriodMs(Duration.ofDays(3).toMillis());
        List<StreamT> streamTList = new ArrayList<>(1);
        streamTList.add(streamT);
        return operationClient.createStreams(streamTList, DEFAULT_REQUEST_TIMEOUT)
            .get()
            .get(0);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        operationClient.close();
    }
}