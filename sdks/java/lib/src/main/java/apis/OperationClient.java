package apis;

import header.AppendResultT;
import header.FetchResultT;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import models.FetchedRecordBatch;
import models.RecordBatch;

public interface OperationClient extends Closeable {
    /**
     * Append a batch to data nodes.
     * @param recordBatch record batch to be appended.
     * @param timeout request timeout.
     * @return AppendResult for this request. Null if the append request failed.
     */
    CompletableFuture<AppendResultT> appendBatch(RecordBatch recordBatch, Duration timeout);
    /**
     * Fetch a batch from data nodes.
     * @param streamId stream id.
     * @param startOffset start offset.
     * @param minBytes minimum bytes to be fetched.
     * @param maxBytes maximum bytes to be fetched.
     * @param timeout request timeout.
     * @return FetchedRecordBatch for this request. Null if the fetch request failed.
     */
    CompletableFuture<FetchedRecordBatch> fetchBatches(long streamId, long startOffset, int minBytes, int maxBytes, Duration timeout);

    /**
     * Get the last offset of a stream.
     * @param streamId stream id.
     * @param timeout request timeout.
     * @return last offset of the stream.
     */
    CompletableFuture<Long> getLastOffset(long streamId, Duration timeout);
    /**
     * Start the client.
     * @throws Exception
     */
    void start() throws Exception;
}
