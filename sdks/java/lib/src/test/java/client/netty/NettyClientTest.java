package client.netty;

import apis.ClientConfigurationBuilder;
import client.protocol.SbpFrame;
import client.protocol.SbpFrameBuilder;
import client.route.Address;
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
    private static DefaultMockNettyServerThread serverThread;

    @BeforeAll
    static void setUp() throws InterruptedException {
        serverThread = new DefaultMockNettyServerThread(serverPort, defaultResponsePrefix);
        serverThread.start();
    }

    @Test
    void testInvokeAsync() throws Exception {
        String requestMessage = "This is Async test";
        int requestStreamId = 111;
        ClientConfigurationBuilder builder = new ClientConfigurationBuilder()
            .setPmEndpoint(String.format("%s:%d", defaultPmAddress.getHost(), defaultPmAddress.getPort()))
            .setConnectionTimeout(Duration.ofSeconds(3))
            .setChannelMaxIdleTime(Duration.ofSeconds(10));
        try (NettyClient client = new NettyClient(builder.build())) {
            client.start();
            SbpFrame sbpFrame = new SbpFrameBuilder()
                .setFlag(SbpFrame.GENERAL_RESPONSE_FLAG)
                .setPayload(new ByteBuffer[] {ByteBuffer.wrap(requestMessage.getBytes(StandardCharsets.ISO_8859_1))})
                .setStreamId(requestStreamId)
                .setOperationCode((short) 10)
                .build();

            SbpFrame responseFrame = client.invokeAsync(client.pmAddress, sbpFrame, Duration.ofSeconds(3)).get();

            Assertions.assertEquals(responseFrame.getFlag(), SbpFrame.GENERAL_RESPONSE_FLAG);
            Assertions.assertEquals(responseFrame.getStreamId(), requestStreamId);
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