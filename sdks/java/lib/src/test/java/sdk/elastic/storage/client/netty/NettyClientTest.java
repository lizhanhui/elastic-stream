package sdk.elastic.storage.client.netty;

import sdk.elastic.storage.apis.ClientConfigurationBuilder;
import sdk.elastic.storage.client.protocol.SbpFrame;
import sdk.elastic.storage.client.protocol.SbpFrameBuilder;
import sdk.elastic.storage.client.route.Address;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class NettyClientTest {
    private static final int serverPort = 8100;
    private static final String defaultResponsePrefix = "hello, world: ";
    private static final Address defaultPmAddress = new Address("localhost", serverPort);
    private static final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "TestHouseKeepingService"));
    private static DefaultMockNettyServerThread serverThread;

    @BeforeAll
    static void setUp() throws InterruptedException {
        serverThread = new DefaultMockNettyServerThread(serverPort, defaultResponsePrefix);
        serverThread.start();
    }

    @Test
    void testInvokeAsync() throws Exception {
        String requestMessage = "This is Async test";
        ClientConfigurationBuilder builder = new ClientConfigurationBuilder()
            .setPmEndpoint(String.format("%s:%d", defaultPmAddress.getHost(), defaultPmAddress.getPort()))
            .setConnectionTimeout(Duration.ofSeconds(3))
            .setChannelMaxIdleTime(Duration.ofSeconds(10));
        try (NettyClient client = new NettyClient(builder.build(), timer)) {
            client.start();
            SbpFrame sbpFrame = new SbpFrameBuilder()
                .setFlag(SbpFrame.GENERAL_RESPONSE_FLAG)
                .setPayload(new ByteBuffer[] {ByteBuffer.wrap(requestMessage.getBytes(StandardCharsets.ISO_8859_1))})
                .setOperationCode((short) 10)
                .build();

            SbpFrame responseFrame = client.invokeAsync(sbpFrame, Duration.ofSeconds(3)).get();

            Assertions.assertEquals(responseFrame.getFlag(), SbpFrame.GENERAL_RESPONSE_FLAG);
            Assertions.assertEquals(responseFrame.getStreamId(), 0L);
            byte[] payload = new byte[responseFrame.payloadLength()];
            int index = 0;
            for (ByteBuffer buffer : responseFrame.getPayload()) {
                buffer.get(payload, index, buffer.remaining());
                index += buffer.remaining();
            }
            String responseMessage = new String(payload, StandardCharsets.ISO_8859_1);
            Assertions.assertEquals(responseMessage, defaultResponsePrefix + requestMessage);
        }
    }

    @AfterAll
    static void tearDown() throws IOException{
        serverThread.close();
    }
}