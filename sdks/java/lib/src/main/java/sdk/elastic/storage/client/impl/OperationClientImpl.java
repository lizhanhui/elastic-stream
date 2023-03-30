package sdk.elastic.storage.client.impl;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.flatbuffers.FlatBufferBuilder;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sdk.elastic.storage.apis.ClientConfiguration;
import sdk.elastic.storage.apis.OperationClient;
import sdk.elastic.storage.apis.exception.ClientException;
import sdk.elastic.storage.apis.manager.ResourceManager;
import sdk.elastic.storage.client.common.ClientId;
import sdk.elastic.storage.client.common.PmUtil;
import sdk.elastic.storage.client.common.ProtocolUtil;
import sdk.elastic.storage.client.common.RemotingUtil;
import sdk.elastic.storage.client.netty.NettyClient;
import sdk.elastic.storage.client.protocol.SbpFrame;
import sdk.elastic.storage.client.route.Address;
import sdk.elastic.storage.flatc.header.AppendInfoT;
import sdk.elastic.storage.flatc.header.AppendRequest;
import sdk.elastic.storage.flatc.header.AppendRequestT;
import sdk.elastic.storage.flatc.header.AppendResponse;
import sdk.elastic.storage.flatc.header.AppendResultT;
import sdk.elastic.storage.flatc.header.ClientRole;
import sdk.elastic.storage.flatc.header.CreateStreamResultT;
import sdk.elastic.storage.flatc.header.FetchInfoT;
import sdk.elastic.storage.flatc.header.FetchRequest;
import sdk.elastic.storage.flatc.header.FetchRequestT;
import sdk.elastic.storage.flatc.header.FetchResponse;
import sdk.elastic.storage.flatc.header.FetchResultT;
import sdk.elastic.storage.flatc.header.HeartbeatRequest;
import sdk.elastic.storage.flatc.header.HeartbeatRequestT;
import sdk.elastic.storage.flatc.header.HeartbeatResponse;
import sdk.elastic.storage.flatc.header.RangeCriteriaT;
import sdk.elastic.storage.flatc.header.RangeIdT;
import sdk.elastic.storage.flatc.header.RangeT;
import sdk.elastic.storage.flatc.header.ReplicaNodeT;
import sdk.elastic.storage.flatc.header.SealRangesResultT;
import sdk.elastic.storage.flatc.header.StreamT;
import sdk.elastic.storage.models.OperationCode;
import sdk.elastic.storage.models.RecordBatch;

import static sdk.elastic.storage.flatc.header.ErrorCode.OK;

public class OperationClientImpl implements OperationClient {
    private static final Logger log = LoggerFactory.getLogger(OperationClientImpl.class);
    private final NettyClient nettyClient;
    private final ResourceManager resourceManager;
    private final StreamRangeCache streamRangeCache;
    private final Duration heartbeatInterval;
    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "HouseKeepingService"));

    private static final Duration CACHE_LOAD_TIMEOUT = Duration.ofSeconds(3);

    protected OperationClientImpl(Duration heartbeatInterval, NettyClient nettyClient,
        ResourceManager resourceManager, StreamRangeCache streamRangeCache) {
        this.heartbeatInterval = heartbeatInterval;
        this.nettyClient = nettyClient;
        this.resourceManager = resourceManager;
        this.streamRangeCache = streamRangeCache;
    }

    public OperationClientImpl(ClientConfiguration clientConfiguration) {
        this.heartbeatInterval = clientConfiguration.getHeartbeatInterval();
        this.nettyClient = new NettyClient(clientConfiguration, timer);
        this.resourceManager = new ResourceManagerImpl(this.nettyClient);
        this.streamRangeCache = new StreamRangeCache(
            new CacheLoader<Long, TreeMap<Long, RangeT>>() {
                @Override
                public TreeMap<Long, RangeT> load(Long streamId) {
                    TreeMap<Long, RangeT> rangesMap = new TreeMap<>();
                    for (RangeT rangeT : fetchRangeArrayBasedOnStreamId(streamId).join()) {
                        rangesMap.put(rangeT.getStartOffset(), rangeT);
                    }
                    return rangesMap;
                }
            });
    }

    @Override
    public CompletableFuture<List<CreateStreamResultT>> createStreams(List<StreamT> streams, Duration timeout) {
        return this.resourceManager.createStreams(streams, timeout);
    }

    @Override
    public CompletableFuture<AppendResultT> appendBatch(RecordBatch recordBatch, Duration timeout) {
        Preconditions.checkArgument(recordBatch != null && recordBatch.getRecords().size() > 0, "Invalid recordBatch since no records were found.");
        long streamId = recordBatch.getBatchMeta().getStreamId();

        return this.streamRangeCache.getLastRange(streamId).thenCompose(rangeT -> {
                ByteBuffer encodedBuffer = recordBatch.encode();

                AppendInfoT appendInfoT = new AppendInfoT();
                appendInfoT.setRequestIndex(0);
                appendInfoT.setBatchLength(encodedBuffer.remaining());
                appendInfoT.setRange(rangeT);
                AppendRequestT appendRequestT = new AppendRequestT();
                appendRequestT.setTimeoutMs((int) timeout.toMillis());
                appendRequestT.setAppendRequests(new AppendInfoT[] {appendInfoT});
                FlatBufferBuilder builder = new FlatBufferBuilder();
                int appendRequestOffset = AppendRequest.pack(builder, appendRequestT);
                builder.finish(appendRequestOffset);

                SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.APPEND, builder.dataBuffer(), new ByteBuffer[] {encodedBuffer});

                Address targetDataNode = null;
                for (ReplicaNodeT node : rangeT.getReplicaNodes()) {
                    if (node.getIsPrimary()) {
                        targetDataNode = Address.fromAddress(node.getDataNode().getAdvertiseAddr());
                        break;
                    }
                }
                assert targetDataNode != null;
                log.debug("trying to append a batch for streamId {} to datanode {}", streamId, targetDataNode.getAddress());

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
                            .thenAccept(sealResultList -> {
                                log.info("sealed for streamId {}, result: {}", streamId, sealResultList.get(0));
                                handleSealRangesResultList(sealResultList);
                            }).join();
                        return new ArrayList<>();
                    });
            }
        ).thenApply(list -> list.isEmpty() ? null : list.get(0));
    }

    @Override
    public CompletableFuture<Long> getLastWritableOffset(long streamId, Duration timeout) {
        return this.streamRangeCache.getLastRange(streamId).thenCompose(rangeT -> {
            RangeIdT rangeIdT = new RangeIdT();
            rangeIdT.setStreamId(rangeT.getStreamId());
            rangeIdT.setRangeIndex(rangeT.getRangeIndex());
            log.debug("trying to get last writable offset for streamId {}", streamId);
            // Any data node is ok.
            Address dataNodeAddress = Address.fromAddress(rangeT.getReplicaNodes()[0].getDataNode().getAdvertiseAddr());
            // The nextOffset in the cache may be out of date, so we need to fetch the latest range info from the PM.
            return this.resourceManager.describeRanges(dataNodeAddress, Collections.singletonList(rangeIdT), timeout)
                .thenCompose(list -> {
                    CompletableFuture<Long> future = new CompletableFuture<>();
                    if (list.get(0).getStatus().getCode() != OK) {
                        future.completeExceptionally(new ClientException("Get last writableOffset failed with code " + list.get(0).getStatus().getCode() + ", msg: " + list.get(0).getStatus().getMessage()));
                        return future;
                    }
                    future.complete(list.get(0).getRange().getNextOffset());
                    return future;
                });
        });
    }

    @Override
    public CompletableFuture<List<RecordBatch>> fetchBatches(long streamId, long startOffset, int minBytes,
        int maxBytes, Duration timeout) {
        return this.streamRangeCache.getFloorRange(streamId, startOffset).thenCompose(rangeT -> {
                // "floor" range may be the higher range.
                long realStartOffset = Math.max(startOffset, rangeT.getStartOffset());
                FetchInfoT fetchInfoT = new FetchInfoT();
                fetchInfoT.setStreamId(streamId);
                fetchInfoT.setRequestIndex(0);
                fetchInfoT.setFetchOffset(realStartOffset);
                fetchInfoT.setBatchMaxBytes(maxBytes);
                FetchRequestT fetchRequestT = new FetchRequestT();
                fetchRequestT.setMaxWaitMs((int) timeout.toMillis());
                fetchRequestT.setMinBytes(minBytes);
                fetchRequestT.setFetchRequests(new FetchInfoT[] {fetchInfoT});
                FlatBufferBuilder builder = new FlatBufferBuilder();
                int fetchRequestOffset = FetchRequest.pack(builder, fetchRequestT);
                builder.finish(fetchRequestOffset);
                SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.FETCH, builder.dataBuffer());

                // No need to fetch from primary node. Fetch from the first node.
                Address targetDataNodeAddress = Address.fromAddress(rangeT.getReplicaNodes()[0].getDataNode().getAdvertiseAddr());
                log.debug("trying to fetch batches from datanode {}", targetDataNodeAddress);
                // TODO: fully implement the fetch logic. Now we just fetch from the floor range.
                return nettyClient.invokeAsync(targetDataNodeAddress, sbpFrame, timeout)
                    .thenCompose(responseFrame -> {
                        FetchResponse response = FetchResponse.getRootAsFetchResponse(responseFrame.getHeader());
                        return extractResponse(response).thenCompose(list -> {
                            CompletableFuture<List<RecordBatch>> future = new CompletableFuture<>();
                            if (list.get(0).getStatus().getCode() != OK) {
                                future.completeExceptionally(new ClientException("Fetch batches failed with code " + list.get(0).getStatus().getCode() + ", msg: " + list.get(0).getStatus().getMessage()));
                                return future;
                            }
                            int count = list.get(0).getBatchCount();
                            future.complete(RecordBatch.decode(responseFrame.getPayload()[0], count));
                            return future;
                        });
                    });
            }
        );
    }

    @Override
    public CompletableFuture<Boolean> heartbeat(Address address, Duration timeout) {
        HeartbeatRequestT heartbeatRequestT = new HeartbeatRequestT();
        heartbeatRequestT.setClientId(getClientId().toString());
        heartbeatRequestT.setClientRole(ClientRole.CLIENT_ROLE_CUSTOMER);
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int heartbeatRequestOffset = HeartbeatRequest.pack(builder, heartbeatRequestT);
        builder.finish(heartbeatRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.HEARTBEAT, builder.dataBuffer());
        return nettyClient.invokeAsync(address, sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                HeartbeatResponse response = HeartbeatResponse.getRootAsHeartbeatResponse(responseFrame.getHeader());
                Address updatePmAddress = PmUtil.extractNewPmAddress(response.status());
                // need to connect to new Pm primary node.
                if (updatePmAddress != null) {
                    nettyClient.updatePmAddress(updatePmAddress);
                    return nettyClient.invokeAsync(address, sbpFrame, timeout).thenCompose(responseFrame2 -> extractResponse(HeartbeatResponse.getRootAsHeartbeatResponse(responseFrame2.getHeader())));
                }

                return extractResponse(response);
            });
    }

    @Override
    public ClientId getClientId() {
        return this.nettyClient.getClientId();
    }

    @Override
    public void start() throws Exception {
        this.nettyClient.start();

        TimerTask timerTaskHeartBeat = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    OperationClientImpl.this.keepNettyChannelsAlive();
                } catch (Throwable e) {
                    log.error("heartbeat exception ", e);
                } finally {
                    timer.newTimeout(this, OperationClientImpl.this.heartbeatInterval.getSeconds(), TimeUnit.SECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskHeartBeat, this.heartbeatInterval.getSeconds(), TimeUnit.SECONDS);
    }

    private CompletableFuture<List<AppendResultT>> extractResponse(AppendResponse response) {
        CompletableFuture<List<AppendResultT>> future = new CompletableFuture<>();
        if (response.status().code() != OK) {
            future.completeExceptionally(new ClientException("Append batch failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return future;
        }
        List<AppendResultT> appendResultList = Arrays.asList(response.unpack().getAppendResponses());

        // invalidate the cache if the append request is successful.
        appendResultList.forEach(appendResultT -> {
            if (appendResultT.getStatus().getCode() == OK) {
                this.streamRangeCache.invalidate(appendResultT.getStreamId());
            }
        });
        future.complete(appendResultList);
        return future;
    }

    private void handleSealRangesResultList(List<SealRangesResultT> sealResultList) {
        sealResultList.forEach(sealResultT -> {
            if (sealResultT.getStatus().getCode() == OK) {
                long streamId = sealResultT.getRange().getStreamId();
                // update the ranges.
                streamRangeCache.getLastRange(streamId)
                    .thenAccept(rangeT -> {
                        // If the same last range is returned, update the last range.
                        if (rangeT.getRangeIndex() == sealResultT.getRange().getRangeIndex()) {
                            streamRangeCache.get(streamId).put(sealResultT.getRange().getStartOffset(), sealResultT.getRange());
                            return;
                        }
                        // If the next range is returned, update the last range and put the new range.
                        if ((rangeT.getRangeIndex() + 1) == sealResultT.getRange().getRangeIndex()) {
                            // It means an empty range is sealed. Just replace it.
                            if (rangeT.getStartOffset() == sealResultT.getRange().getStartOffset()) {
                                streamRangeCache.get(streamId).put(sealResultT.getRange().getStartOffset(), sealResultT.getRange());
                                return;
                            }
                            rangeT.setEndOffset(sealResultT.getRange().getStartOffset());
                            rangeT.setNextOffset(sealResultT.getRange().getStartOffset());
                            streamRangeCache.get(streamId).put(sealResultT.getRange().getStartOffset(), sealResultT.getRange());
                            return;
                        }
                        // In other cases, just erase the cached map.
                        streamRangeCache.invalidate(streamId);
                    })
                    .join();
            }
        });
    }

    private CompletableFuture<Boolean> extractResponse(HeartbeatResponse response) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (response.status().code() != OK) {
            future.completeExceptionally(new ClientException("Heartbeat failed with code " + response.status().code() + ", msg: " + response.status().message() + ", clientId: " + response.clientId()));
            return future;
        }
        log.debug("Heartbeat success, clientId: {}", response.clientId());
        future.complete(true);
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

    private CompletableFuture<RangeT[]> fetchRangeArrayBasedOnStreamId(long streamId) {
        RangeCriteriaT rangeCriteriaT = new RangeCriteriaT();
        // no need to setDataNode since we only need to get the ranges based on streamId.
        rangeCriteriaT.setStreamId(streamId);
        return resourceManager.listRanges(Collections.singletonList(rangeCriteriaT), CACHE_LOAD_TIMEOUT)
            .thenApply(list -> list.get(0).getRanges());
    }

    /**
     * Keep the netty channels alive by sending heartbeat request to the server.
     */
    private void keepNettyChannelsAlive() {
        for (Map.Entry<Channel, Boolean> item : this.nettyClient.aliveChannelTable.entrySet()) {
            if (item.getValue()) {
                Channel channel = item.getKey();
                Address address = RemotingUtil.parseChannelRemoteAddress(channel);
                try {
                    this.heartbeat(address, Duration.ofSeconds(3)).join();
                    log.debug("Successfully refresh channel with address {} ", address);
                } catch (Throwable ex) {
                    log.error("Failed to refresh channel with address {} ", address, ex);
                }
            }
        }
    }

    /**
     * Generate SbpFrames for Append based on provided recordBatches.
     * Note that recordBatches with the same streamId are grouped into the same SbpFrame.
     *
     * @param recordBatches recordBatches to be sent to server. They may contain different streamId.
     * @param timeoutMillis timeout for each AppendRequest.
     * @return Map of streamId to SbpFrame.
     */
    private Map<Long, SbpFrame> generateAppendRequest(List<RecordBatch> recordBatches, int timeoutMillis) {
        // streamId -> List<AppendInfoT>
        Map<Long, List<AppendInfoT>> appendInfoTMap = new HashMap<>();
        // streamId -> request_index
        Map<Long, Integer> appendInfoIndexMap = new HashMap<>();
        // streamId -> payloadList
        Map<Long, List<ByteBuffer>> payloadMap = new HashMap<>();

        for (RecordBatch batch : recordBatches) {
            // no need to send empty batch
            if (batch.getRecords() == null || batch.getRecords().size() == 0) {
                continue;
            }
            Long streamId = batch.getBatchMeta().getStreamId();

            AppendInfoT appendInfoT = new AppendInfoT();
            ByteBuffer encodedBuffer = batch.encode();
            this.streamRangeCache.getLastRange(streamId).thenAccept(appendInfoT::setRange).join();
            appendInfoT.setBatchLength(encodedBuffer.remaining());

            // find the request index in the appendRequest for this batch
            int index = appendInfoIndexMap.getOrDefault(streamId, 0);
            appendInfoT.setRequestIndex(index);
            appendInfoIndexMap.put(streamId, index + 1);

            // add to the right batch list
            appendInfoTMap.computeIfAbsent(streamId, key -> new ArrayList<>())
                .add(appendInfoT);
            payloadMap.computeIfAbsent(streamId, key -> new ArrayList<>())
                .add(encodedBuffer);
        }

        Map<Long, SbpFrame> streamIdToSbpFrameMap = new HashMap<>(appendInfoTMap.size());
        appendInfoTMap.forEach((streamId, appendInfoTList) -> {
            AppendRequestT appendRequestT = new AppendRequestT();
            appendRequestT.setTimeoutMs(timeoutMillis);
            appendRequestT.setAppendRequests(appendInfoTList.toArray(new AppendInfoT[0]));

            FlatBufferBuilder builder = new FlatBufferBuilder();
            int pack = AppendRequest.pack(builder, appendRequestT);
            builder.finish(pack);

            SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.APPEND, builder.dataBuffer(), payloadMap.get(streamId).toArray(new ByteBuffer[0]));
            streamIdToSbpFrameMap.put(streamId, sbpFrame);
        });

        return streamIdToSbpFrameMap;
    }

    @Override
    public void close() throws IOException {
        this.timer.stop();
        this.nettyClient.close();
    }
}
