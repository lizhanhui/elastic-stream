package client.impl.manager;

import apis.ClientConfigurationBuilder;
import apis.manager.ResourceManager;
import client.netty.NettyClient;
import client.route.Address;
import header.CreateStreamResultT;
import header.StreamT;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static client.protocol.SbpFrame.ENDING_RESPONSE_FLAG;
import static client.protocol.SbpFrame.GENERAL_RESPONSE_FLAG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ResourceManagerImplTest {
    private static final int serverPort = 2378;
    private static final Address defaultPmAddress = new Address("114.116.40.8", serverPort);
    private static ResourceManager resourceManager;

    @BeforeAll
    static void setUp() throws Exception {
        ClientConfigurationBuilder builder = new ClientConfigurationBuilder()
            .setPmEndpoint(String.format("%s:%d", defaultPmAddress.getHost(), defaultPmAddress.getPort()))
            .setConnectionTimeout(Duration.ofSeconds(3))
            .setChannelMaxIdleTime(Duration.ofSeconds(10));
        NettyClient nettyClient = new NettyClient(builder.build());
        nettyClient.start();
        resourceManager = new ResourceManagerImpl(nettyClient);
    }

    @Test
    void createStreams() throws ExecutionException, InterruptedException {
        StreamT streamT = new StreamT();
        streamT.setStreamId(0L);
        streamT.setReplicaNums((byte) 2);
        streamT.setRetentionPeriodMs(Duration.ofDays(3).toMillis());
        List<StreamT> streamTList = new ArrayList<>(1);
        streamTList.add(streamT);
        List<CreateStreamResultT> resultList = resourceManager.createStreams(streamTList, Duration.ofSeconds(3)).get();
        assertEquals(1, resultList.size());

        StreamT resultStream = resultList.get(0).getStream();
        assertNotEquals(0L, resultStream.getStreamId());
        assertEquals(2, resultStream.getReplicaNums());
        assertEquals(Duration.ofDays(3).toMillis(), resultStream.getRetentionPeriodMs());
    }

    @Test
    void pingPong() throws ExecutionException, InterruptedException {
        Byte flag = resourceManager.pingPong(Duration.ofSeconds(3)).get();
        assertEquals(flag, (byte) (GENERAL_RESPONSE_FLAG | ENDING_RESPONSE_FLAG));
    }

    @AfterAll
    static void tearDown() throws Exception {
        resourceManager.close();
    }
}