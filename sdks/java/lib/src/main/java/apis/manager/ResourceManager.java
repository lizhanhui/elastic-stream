package apis.manager;

import header.CreateStreamResultT;
import header.ListRangesResultT;
import header.RangeCriteriaT;
import header.StreamT;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ResourceManager extends Closeable {
    /**
     * Send create streams request to the PM.
     *
     * @param streams basic streams' messages
     * @param timeout request timeout
     * @return create stream results
     */
    CompletableFuture<List<CreateStreamResultT>> createStreams(List<StreamT> streams, Duration timeout);

    /**
     * Send list ranges request to the PM.
     * @param rangeCriteriaList
     * @param timeout
     * @return
     */
    CompletableFuture<List<ListRangesResultT>> listRanges(List<RangeCriteriaT> rangeCriteriaList, Duration timeout);

    /**
     * Ping the PM.
     * @param timeout request timeout
     * @return pong flag
     */
    CompletableFuture<Byte> pingPong(Duration timeout);

}
