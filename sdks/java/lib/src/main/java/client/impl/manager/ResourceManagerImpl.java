package client.impl.manager;

import apis.manager.ResourceManager;
import client.cache.StreamRangeCache;
import client.netty.NettyClient;
import client.protocol.SbpFrame;
import client.protocol.SbpFrameBuilder;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.flatbuffers.FlatBufferBuilder;
import header.AppendInfoT;
import header.AppendRequest;
import header.AppendRequestT;
import header.CreateStreamResultT;
import header.CreateStreamsRequest;
import header.CreateStreamsRequestT;
import header.CreateStreamsResponse;
import header.ListRangesResultT;
import header.Range;
import header.RangeCriteriaT;
import header.StreamT;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import models.OperationCode;
import models.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import records.RecordBatchMeta;

public class ResourceManagerImpl implements ResourceManager {
    private static final Logger log = LoggerFactory.getLogger(ResourceManagerImpl.class);
    private final NettyClient nettyClient;
    private final StreamRangeCache streamRangeCache;

    public ResourceManagerImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;

        this.streamRangeCache = new StreamRangeCache(
            new CacheLoader<Long, List<Range>>() {
                @Override
                public List<Range> load(Long streamId) {
                    return null;
                }
            });
    }

    @Override
    public CompletableFuture<List<ListRangesResultT>> listRanges(List<RangeCriteriaT> rangeCriteriaList,
        Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Byte> pingPong(Duration timeout) {
        SbpFrame sbpFrame = constructRequestSbpFrame(OperationCode.PING, 0, ByteBuffer.wrap(new byte[] {1,2,3}), new ByteBuffer[]{ByteBuffer.wrap(new byte[] {4,5,6})});
        return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame -> CompletableFuture.completedFuture(responseFrame.getFlag()));
    }

    @Override
    public CompletableFuture<List<CreateStreamResultT>> createStreams(List<StreamT> streams, Duration timeout) {
        Preconditions.checkArgument(streams != null && streams.size() > 0, "Invalid streams since no streams were found.");

        CreateStreamsRequestT createStreamsRequestT = new CreateStreamsRequestT();
        createStreamsRequestT.setStreams(streams.toArray(new StreamT[0]));
        createStreamsRequestT.setTimeoutMs((int) timeout.toMillis());
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int createStreamsRequestOffset = CreateStreamsRequest.pack(builder, createStreamsRequestT);
        builder.finish(createStreamsRequestOffset);

        SbpFrame sbpFrame = constructRequestSbpFrame(OperationCode.CREATE_STREAMS, 0, builder.dataBuffer());
        return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame -> {
            List<CreateStreamResultT> resultList = new ArrayList<>();
            CreateStreamsResponse response = CreateStreamsResponse.getRootAsCreateStreamsResponse(responseFrame.getHeader());
            for (int i = 0; i < response.createResponsesVector().length(); i++) {
                resultList.add(response.createResponsesVector().get(i).unpack());
            }
            return CompletableFuture.completedFuture(resultList);
        });
    }

    /**
     * Generate AppendRequests based on provided recordBatches.
     * Note that recordBatches with the same streamId are grouped into the same AppendRequest.
     *
     * @param recordBatches recordBatches to be sent to server. They may contain different streamId.
     * @param timeoutMillis timeout for each AppendRequest.
     * @return Map of streamId to AppendRequest.
     */
    private Map<Long, AppendRequest> generateAppendRequest(List<RecordBatch> recordBatches, int timeoutMillis) {
        // streamId -> List<AppendInfoT>
        Map<Long, List<AppendInfoT>> appendInfoTMap = new HashMap<>();
        // streamId -> request_index
        Map<Long, Integer> appendInfoIndexMap = new HashMap<>();

        for (RecordBatch batch : recordBatches) {
            // no need to send empty batch
            if (batch.getRecords() == null || batch.getRecords().size() == 0) {
                continue;
            }
            RecordBatchMeta batchMeta = RecordBatchMeta.getRootAsRecordBatchMeta(batch.getBatchMeta().duplicate());
            Long streamId = batchMeta.streamId();

            AppendInfoT appendInfoT = new AppendInfoT();
            appendInfoT.setStreamId(streamId);
            appendInfoT.setBatchLength(batch.getEncodeLength());

            // find the request index in the appendRequest for this batch
            int index = appendInfoIndexMap.getOrDefault(streamId, 0);
            appendInfoT.setRequestIndex(index);
            appendInfoIndexMap.put(streamId, index + 1);

            // add to the right batch list
            appendInfoTMap.computeIfAbsent(streamId, key -> new ArrayList<>())
                .add(appendInfoT);
        }

        Map<Long, AppendRequest> streamIdToAppendRequestMap = new HashMap<>();
        appendInfoTMap.forEach((streamId, appendInfoTList) -> {
            AppendRequestT appendRequestT = new AppendRequestT();
            appendRequestT.setTimeoutMs(timeoutMillis);
            appendRequestT.setAppendRequests(appendInfoTList.toArray(new AppendInfoT[0]));

            FlatBufferBuilder builder = new FlatBufferBuilder();
            int pack = AppendRequest.pack(builder, appendRequestT);
            builder.finish(pack);
            streamIdToAppendRequestMap.put(streamId, AppendRequest.getRootAsAppendRequest(builder.dataBuffer()));
        });

        return streamIdToAppendRequestMap;
    }

    private Range getLastRange(long streamId) {
        List<Range> ranges = this.streamRangeCache.get(streamId);
        if (ranges == null) {
            // try again.
            ranges = this.streamRangeCache.get(streamId);
        }
        return (ranges == null || ranges.size() == 0) ? null : ranges.get(ranges.size() - 1);
    }

    private SbpFrame constructRequestSbpFrame(OperationCode operationCode, int streamId, ByteBuffer header) {
        return constructRequestSbpFrame(operationCode, streamId, header, null);
    }

    private SbpFrame constructRequestSbpFrame(OperationCode operationCode, int streamId, ByteBuffer header,
        ByteBuffer[] payloads) {
        return new SbpFrameBuilder()
            .setFlag(SbpFrame.DEFAULT_REQUEST_FLAG)
            .setStreamId(streamId)
            .setOperationCode(operationCode.getCode())
            .setHeader(header)
            .setPayload(payloads)
            .build();
    }

    @Override
    public void close() throws IOException {
        nettyClient.close();
    }
}
