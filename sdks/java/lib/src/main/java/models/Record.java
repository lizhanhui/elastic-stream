package models;

import java.nio.ByteBuffer;
import java.util.Map;

public class Record {
    private long streamId;
    private Headers headers;
    private Map<String, String> properties;
    private ByteBuffer body;

    public Record(long streamId, Headers headers, Map<String, String> properties, ByteBuffer body) {
        this.streamId = streamId;
        this.headers = headers;
        this.properties = properties;
        this.body = body;
    }

    public long getStreamId() {
        return streamId;
    }

    public Headers getHeaders() {
        return headers;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ByteBuffer getBody() {
        return body;
    }
}
