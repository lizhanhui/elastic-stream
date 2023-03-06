package apis.writer;

import models.RecordMetadata;
import models.WriterRecords;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for the {@link Writer}.
 * Provides a way to append record batches to the storage.
 */
public interface Writer extends Closeable {
    /**
     * Appends a record batch to the storage asynchronously.
     * <p>
     * The returned future will be completed when the record batch is successfully appended to the storage,
     * or an exception is thrown.
     *
     * @param writerRecords the record batch to append
     * @return a future that contains the record metadata
     */
    CompletableFuture<RecordMetadata> append(WriterRecords writerRecords);
}
