package sdk.elastic.storage.client.common;

import sdk.elastic.storage.client.protocol.SbpFrame;
import sdk.elastic.storage.client.protocol.SbpFrameBuilder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import sdk.elastic.storage.models.HeaderKey;
import sdk.elastic.storage.models.OperationCode;
import sdk.elastic.storage.flatc.records.KeyValueT;

public class ProtocolUtil {
    /**
     * Construct a request SbpFrame
     *
     * @param operationCode operation code
     * @param header        header
     * @return generated SbpFrame
     */
    public static SbpFrame constructRequestSbpFrame(OperationCode operationCode, ByteBuffer header) {
        return constructRequestSbpFrame(operationCode, header, null);
    }

    /**
     * Construct a request SbpFrame
     *
     * @param operationCode operation code
     * @param header        header
     * @param payloads      payloads, can be null
     * @return generated SbpFrame
     */
    public static SbpFrame constructRequestSbpFrame(OperationCode operationCode, ByteBuffer header,
        ByteBuffer[] payloads) {
        return new SbpFrameBuilder()
            .setFlag(SbpFrame.DEFAULT_REQUEST_FLAG)
            .setOperationCode(operationCode.getCode())
            .setHeader(header)
            .setPayload(payloads)
            .build();
    }

    public static Map<String, String> keyValueTList2Map(List<KeyValueT> list) {
        return list.stream()
            .collect(Collectors.toMap(KeyValueT::getKey, KeyValueT::getValue));
    }

    public static KeyValueT[] map2KeyValueTList(Map<String, String> map) {
        return map.entrySet()
            .stream()
            .map(entry -> {
                KeyValueT item = new KeyValueT();
                item.setKey(entry.getKey());
                item.setValue(entry.getValue());
                return item;
            }).toArray(KeyValueT[]::new);
    }

    public static Map<HeaderKey, String> keyValueTList2HeaderMap(List<KeyValueT> list) {
        return list.stream()
            .collect(Collectors.toMap(item -> HeaderKey.valueOf(item.getKey()), KeyValueT::getValue));
    }
}
