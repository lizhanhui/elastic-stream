package client.impl;

import apis.ClientConfiguration;
import apis.OperationClient;
import apis.exception.ClientException;
import apis.manager.ResourceManager;
import client.cache.StreamRangeCache;
import client.common.ProtocolUtil;
import client.impl.manager.ResourceManagerImpl;
import client.netty.NettyClient;
import client.protocol.SbpFrame;
import client.route.Address;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.flatbuffers.FlatBufferBuilder;
import header.AppendInfoT;
import header.AppendRequestT;
import header.AppendResponse;
import header.AppendResultT;
import header.FetchInfoT;
import header.FetchRequestT;
import header.FetchResponse;
import header.FetchResultT;
import header.RangeCriteriaT;
import header.RangeIdT;
import header.RangeT;
import header.ReplicaNodeT;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import models.FetchedRecordBatch;
import models.OperationCode;
import models.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static header.ErrorCode.OK;

public class OperationClientImpl implements OperationClient {
    private static final Logger log = LoggerFactory.getLogger(OperationClientImpl.class);
    private final NettyClient nettyClient;
    private final ResourceManager resourceManager;
    private final StreamRangeCache streamRangeCache;

    private static final Duration cacheLoadTimeout = Duration.ofSeconds(3);

    public OperationClientImpl(ClientConfiguration clientConfiguration) {
        this.nettyClient = new NettyClient(clientConfiguration);
        this.resourceManager = new ResourceManagerImpl(this.nettyClient);
        this.streamRangeCache = new StreamRangeCache(
            new CacheLoader<Long, List<RangeT>>() {
                @Override
                public List<RangeT> load(Long streamId) {
                    return fetchRangeTListBasedOnStreamId(streamId).join();
                }
            });
    }

    @Override
    public CompletableFuture<AppendResultT> appendBatch(RecordBatch recordBatch, Duration timeout) {
        Preconditions.checkArgument(recordBatch != null && recordBatch.getRecords().size() > 0, "Invalid recordBatch since no records were found.");

        long streamId = recordBatch.getBatchMeta().getStreamId();

        AppendInfoT appendInfoT = new AppendInfoT();
        appendInfoT.setRequestIndex(0);
        appendInfoT.setStreamId(streamId);
        appendInfoT.setBatchLength(recordBatch.getEncodeLength());
        AppendRequestT appendRequestT = new AppendRequestT();
        appendRequestT.setTimeoutMs((int) timeout.toMillis());
        appendRequestT.setAppendRequests(new AppendInfoT[] {appendInfoT});
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int appendRequestOffset = header.AppendRequest.pack(builder, appendRequestT);
        builder.finish(appendRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.APPEND, (int) streamId, builder.dataBuffer(), new ByteBuffer[] {recordBatch.encode()});
        return getLastRangeOrCreateOne(streamId).thenCompose(rangeT -> {
                Address targetDataNode = null;
                for (ReplicaNodeT node : rangeT.getReplicaNodes()) {
                    if (node.getIsPrimary()) {
                        targetDataNode = Address.fromAddress(node.getDataNode().getAdvertiseAddr());
                        break;
                    }
                }
                log.debug("trying to append a batch for streamId {} to datanode {}", streamId, targetDataNode);
                return nettyClient.invokeAsync(targetDataNode, sbpFrame, timeout)
                    .thenCompose(responseFrame -> {
                        AppendResponse response = AppendResponse.getRootAsAppendResponse(responseFrame.getHeader());
                        return extractResponse(response);
                    }).exceptionally(e -> {
                        log.error("Failed to append a batch for streamId {}. Try to seal it. Exception detail: ", streamId, e);
                        RangeIdT rangeIdT = new RangeIdT();
                        rangeIdT.setStreamId(streamId);
                        rangeIdT.setRangeIndex(0);
                        // seal the range now.
                        resourceManager.sealRanges(Collections.singletonList(rangeIdT), timeout)
                            .thenAccept(sealResult -> {
                                log.info("seal result for streamId {}, result: {}", streamId, sealResult.get(0));
                                // update the ranges.
                                streamRangeCache.put(streamId, Arrays.asList(sealResult.get(0).getRanges()));
                            }).join();
                        return new ArrayList<>();
                    });
            }
        ).thenApply(list -> list.isEmpty() ? null : list.get(0));
    }

    @Override
    public CompletableFuture<Long> getLastOffset(long streamId, Duration timeout) {
        return getLastRangeOrCreateOne(streamId).thenApply(RangeT::getNextOffset);
    }

    @Override
    public CompletableFuture<FetchedRecordBatch> fetchBatches(long streamId, long startOffset, int minBytes,
        int maxBytes, Duration timeout) {
        FetchInfoT fetchInfoT = new FetchInfoT();
        fetchInfoT.setStreamId(streamId);
        fetchInfoT.setRequestIndex(0);
        fetchInfoT.setFetchOffset(startOffset);
        fetchInfoT.setBatchMaxBytes(maxBytes);
        FetchRequestT fetchRequestT = new FetchRequestT();
        fetchRequestT.setMaxWaitMs((int) timeout.toMillis());
        fetchRequestT.setMinBytes(minBytes);
        fetchRequestT.setFetchRequests(new FetchInfoT[] {fetchInfoT});
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int fetchRequestOffset = header.FetchRequest.pack(builder, fetchRequestT);
        builder.finish(fetchRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.FETCH, (int) streamId, builder.dataBuffer());
        return getLastRangeOrCreateOne(streamId).thenCompose(rangeT -> {
                // No need to fetch from primary node. Fetch from the first node.
                Address targetDataNode = Address.fromAddress(rangeT.getReplicaNodes()[0].getDataNode().getAdvertiseAddr());
                log.debug("trying to fetch batches from datanode {}", targetDataNode);
                return nettyClient.invokeAsync(targetDataNode, sbpFrame, timeout)
                    .thenCompose(responseFrame -> {
                        FetchResponse response = FetchResponse.getRootAsFetchResponse(responseFrame.getHeader());
                        return extractResponse(response).thenApply(list -> {
                            int length = list.get(0).getBatchLength();
                            // TODO: make sure about the response format.
                            if (length != responseFrame.getPayload()[0].remaining()) {
                                throw new RuntimeException("Batch length is not equal to the actual length.");
                            }
                            return new FetchedRecordBatch(streamId, startOffset, responseFrame.getPayload()[0]);
                        });
                    });
            }
        );
    }

    @Override
    public void start() throws Exception {
        this.nettyClient.start();
    }

    private CompletableFuture<RangeT> getLastRangeOrCreateOne(long streamId) {
        CompletableFuture<RangeT> completableFuture = new CompletableFuture<>();
        try {
            List<RangeT> ranges = this.streamRangeCache.get(streamId);

            RangeT targetRange = ranges.get(ranges.size() - 1);
            // Data nodes may have been ready. Try again.
            if (targetRange.getReplicaNodes().length == 0) {
                this.streamRangeCache.refresh(streamId);
                targetRange = this.streamRangeCache.get(streamId).get(ranges.size() - 1);
            }
            if (targetRange.getReplicaNodes().length == 0) {
                completableFuture.completeExceptionally(new ClientException("Failed to get ReplicaNodes of the last range of stream " + streamId));
                return completableFuture;
            }
            // TODO: the last range is sealed. A new range is needed.
//            if (targetRange.getEndOffset() <= 0) {
//
//            }
            completableFuture.complete(targetRange);
        } catch (Throwable ex) {
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    private CompletableFuture<List<AppendResultT>> extractResponse(AppendResponse response) {
        CompletableFuture<List<AppendResultT>> future = new CompletableFuture<>();
        if (response.status().code() != OK) {
            future.completeExceptionally(new ClientException("Append batch failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return future;
        }
        future.complete(Arrays.asList(response.unpack().getAppendResponses()));
        return future;
    }

    private CompletableFuture<List<FetchResultT>> extractResponse(FetchResponse response) {
        CompletableFuture<List<FetchResultT>> future = new CompletableFuture<>();
        if (response.status().code() != OK) {
            future.completeExceptionally(new ClientException("Fetch batches failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return future;
        }
        future.complete(Arrays.asList(response.unpack().getFetchResponses()));
        return future;
    }

    private CompletableFuture<List<RangeT>> fetchRangeTListBasedOnStreamId(long streamId) {
        RangeCriteriaT rangeCriteriaT = new RangeCriteriaT();
        // no need to setDataNode since we only need to get the ranges based on streamId.
        rangeCriteriaT.setStreamId(streamId);
        return resourceManager.listRanges(Collections.singletonList(rangeCriteriaT), cacheLoadTimeout)
            .thenApply(list -> Arrays.asList(list.get(0).getRanges()));
    }

    @Override
    public void close() throws IOException {
        this.nettyClient.close();
    }
}
