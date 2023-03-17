package apis;

import header.AppendResultT;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import models.RecordBatch;

public interface OperationClient extends Closeable {
    /**
     * Append batches to data nodes.
     * @param recordBatch record batch to be appended.
     * @param timeout request timeout.
     * @return AppendResult for this request.
     */
    CompletableFuture<AppendResultT> appendBatch(RecordBatch recordBatch, Duration timeout);
}
