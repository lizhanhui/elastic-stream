package models;

import java.util.List;

public class RecordBatch {
    private long streamId;
    private long baseTimestamp;
    private List<Record> records;

    public RecordBatch(long streamId, long baseTimestamp, List<Record> records) {
        this.streamId = streamId;
        this.baseTimestamp = baseTimestamp;
        this.records = records;
    }

    public RecordBatch(long streamId, List<Record> records) {
        this(streamId, System.currentTimeMillis(), records);
    }
}
