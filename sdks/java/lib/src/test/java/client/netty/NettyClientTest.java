package client.netty;

import apis.ClientConfigurationBuilder;
import client.protocol.SbpFrame;
import client.protocol.SbpFrameBuilder;
import client.route.Address;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class NettyClientTest {
    private static final int serverPort = 8100;
    private static DefaultMockNettyServer nettyServer;
    private static final String defaultResponsePrefix = "hello, world: ";
    private static final Address defaultPmAddress = new Address("localhost", serverPort);
    private static Thread serverThread;

    @BeforeAll
    static void setUp() throws InterruptedException {
        serverThread = new Thread(() -> {
            try {
                nettyServer = new DefaultMockNettyServer(1, serverPort, defaultResponsePrefix);
                nettyServer.start();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        serverThread.start();

        // make sure server gets off the ground
        Thread.sleep(2000);
    }

    @Test
    void testInvokeSync() throws Exception {
        String requestMessage = "This is Sync test";
        int requestStreamId = 110;
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
                .setOperationCode((short) 1)
                .build();

            SbpFrame frame = (SbpFrame) client.invokeSync(client.pmAddress, sbpFrame, 3000);
            Assertions.assertEquals(frame.getFlag(), SbpFrame.GENERAL_RESPONSE_FLAG);
            Assertions.assertEquals(frame.getStreamId(), requestStreamId);

            byte[] payload = new byte[frame.payloadLength()];
            int index = 0;
            for (ByteBuffer buffer : frame.getPayload()) {
                buffer.mark();
                buffer.get(payload, index, buffer.remaining());
                buffer.reset();
                index += buffer.remaining();
            }
            String responseMessage = new String(payload, StandardCharsets.ISO_8859_1);
            Assertions.assertEquals(responseMessage, defaultResponsePrefix + requestMessage);
        }
    }

    @Test
    void testInvokeAsync() throws Exception {
        String requestMessage = "This is Async test";
        int requestStreamId = 111;
        ClientConfigurationBuilder builder = new ClientConfigurationBuilder()
            .setPmEndpoint(String.format("%s:%d", defaultPmAddress.getHost(), defaultPmAddress.getPort()))
            .setConnectionTimeout(Duration.ofSeconds(3))
            .setChannelMaxIdleTime(Duration.ofSeconds(10));
        AtomicReference<SbpFrame> responseFrame = new AtomicReference<>(null);
        try (NettyClient client = new NettyClient(builder.build())) {
            client.start();
            SbpFrame sbpFrame = new SbpFrameBuilder()
                .setFlag(SbpFrame.GENERAL_RESPONSE_FLAG)
                .setPayload(new ByteBuffer[] {ByteBuffer.wrap(requestMessage.getBytes(StandardCharsets.ISO_8859_1))})
                .setStreamId(requestStreamId)
                .setOperationCode((short) 1)
                .build();

            client.invokeAsync(client.pmAddress, sbpFrame, 3000, responseFuture -> {
                responseFrame.set(responseFuture.getResponseSbpFrame());
            });

            // wait for async response
            Thread.sleep(2000);

            Assertions.assertEquals(responseFrame.get().getFlag(), SbpFrame.GENERAL_RESPONSE_FLAG);
            Assertions.assertEquals(responseFrame.get().getStreamId(), requestStreamId);
            byte[] payload = new byte[responseFrame.get().payloadLength()];
            int index = 0;
            for (ByteBuffer buffer : responseFrame.get().getPayload()) {
                buffer.mark();
                buffer.get(payload, index, buffer.remaining());
                buffer.reset();
                index += buffer.remaining();
            }
            String responseMessage = new String(payload, StandardCharsets.ISO_8859_1);
            Assertions.assertEquals(responseMessage, defaultResponsePrefix + requestMessage);
        }
    }

    @AfterAll
    static void tearDown() throws IOException, InterruptedException {
        nettyServer.close();
        Thread.sleep(1000);
        serverThread.interrupt();
    }
}