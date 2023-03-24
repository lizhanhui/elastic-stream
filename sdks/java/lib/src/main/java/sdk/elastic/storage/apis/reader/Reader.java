package sdk.elastic.storage.apis.reader;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import sdk.elastic.storage.models.Record;

public interface Reader extends Closeable {
    /**
     * Fetches a batch of records from the storage asynchronously.
     *
     * @return a future that contains the record batch
     */
    CompletableFuture<List<Record>> fetch();

    /**
     * Seek to the given offset. The next call to {@link #fetch()} will follow the new offset.
     *
     * @param offset the offset to seek to
     */
    void seek(long offset);

    /**
     * Returns the min offset of the related stream to the reader.
     *
     * @return the min offset
     */
    long minOffset();

    /**
     * Returns the max offset of the related stream to the reader.
     *
     * @return the max offset
     */
    long maxOffset();
}
