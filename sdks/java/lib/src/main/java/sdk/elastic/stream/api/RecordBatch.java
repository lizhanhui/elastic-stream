package sdk.elastic.stream.api;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Record batch.
 */
public interface RecordBatch {

    /**
     * Get payload record count.
     *
     * @return record count.
     */
    int count();

    /**
     * Get min timestamp of records.
     *
     * @return min timestamp of records.
     */
    long baseTimestamp();

    /**
     * Get record batch extension properties.
     *
     * @return batch extension properties.
     */
    List<KeyValue> properties();

    /**
     * Get raw payload.
     *
     * @return raw payload.
     */
    ByteBuffer rawPayload();
}
