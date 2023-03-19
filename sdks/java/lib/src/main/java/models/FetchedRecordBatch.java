package models;

import java.nio.ByteBuffer;

public class FetchedRecordBatch {
    private long streamId;
    private long offset;
    private ByteBuffer data;

    public FetchedRecordBatch(long streamId, long offset, ByteBuffer data) {
        this.streamId = streamId;
        this.offset = offset;
        this.data = data;
    }

    public long getStreamId() {
        return streamId;
    }

    public long getOffset() {
        return offset;
    }

    public ByteBuffer getData() {
        return data;
    }
}
