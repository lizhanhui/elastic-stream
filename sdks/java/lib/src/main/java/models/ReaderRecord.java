package models;

import java.nio.ByteBuffer;
import java.util.Map;

public class ReaderRecord extends Record {
    private long offset;
    private long appendTimestamp;

    public ReaderRecord(long streamId, Headers headers, Map<String, String> properties, ByteBuffer body, long offset, long appendTimestamp) {
        super(streamId, headers, properties, body);
        this.offset = offset;
        this.appendTimestamp = appendTimestamp;
    }

    public long getOffset() {
        return offset;
    }

    public long getAppendTimestamp() {
        return appendTimestamp;
    }
}

