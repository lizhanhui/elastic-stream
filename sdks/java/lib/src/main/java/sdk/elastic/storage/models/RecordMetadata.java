package sdk.elastic.storage.models;

/**
 * metadata of a record, which can be located in data nodes.
 */
public class RecordMetadata {
    private long offset;
    private long appendTimestamp;
    private long streamId;
}
