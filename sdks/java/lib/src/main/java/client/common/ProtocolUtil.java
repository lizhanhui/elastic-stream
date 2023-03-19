package client.common;

import client.protocol.SbpFrame;
import client.protocol.SbpFrameBuilder;
import java.nio.ByteBuffer;
import models.OperationCode;

public class ProtocolUtil {
    /**
     * Construct a request SbpFrame
     * @param operationCode operation code
     * @param streamId stream id
     * @param header header
     * @return generated SbpFrame
     */
    public static SbpFrame constructRequestSbpFrame(OperationCode operationCode, int streamId, ByteBuffer header) {
        return constructRequestSbpFrame(operationCode, streamId, header, null);
    }

    /**
     * Construct a request SbpFrame
     * @param operationCode operation code
     * @param streamId stream id
     * @param header header
     * @param payloads payloads, can be null
     * @return generated SbpFrame
     */
    public static SbpFrame constructRequestSbpFrame(OperationCode operationCode, int streamId, ByteBuffer header,
        ByteBuffer[] payloads) {
        return new SbpFrameBuilder()
            .setFlag(SbpFrame.DEFAULT_REQUEST_FLAG)
            .setStreamId(streamId)
            .setOperationCode(operationCode.getCode())
            .setHeader(header)
            .setPayload(payloads)
            .build();
    }
}
