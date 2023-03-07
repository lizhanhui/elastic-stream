package models;

import java.util.List;

public class RecordBatch {
    private long streamId;
    private long baseTimestamp;
    private List<Record> records;
}
