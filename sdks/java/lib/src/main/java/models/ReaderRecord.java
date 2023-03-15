package models;

import java.nio.ByteBuffer;
import java.util.Map;

public class ReaderRecord extends Record {
    private long offset;
    private long appendTimestamp;

    public ReaderRecord(ByteBuffer meta, ByteBuffer body, long offset, long appendTimestamp) {
        super(meta, body);
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

