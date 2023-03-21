package client.impl.manager;

import apis.exception.ClientException;
import apis.manager.ResourceManager;
import client.common.PmUtil;
import client.common.ProtocolUtil;
import client.netty.NettyClient;
import client.protocol.SbpFrame;
import client.route.Address;
import com.google.common.base.Preconditions;
import com.google.flatbuffers.FlatBufferBuilder;
import header.AppendInfoT;
import header.AppendRequest;
import header.AppendRequestT;
import header.CreateStreamResultT;
import header.CreateStreamsRequest;
import header.CreateStreamsRequestT;
import header.CreateStreamsResponse;
import header.DescribeRangeResultT;
import header.DescribeRangesRequest;
import header.DescribeRangesRequestT;
import header.DescribeRangesResponse;
import header.DescribeStreamResultT;
import header.DescribeStreamsRequest;
import header.DescribeStreamsRequestT;
import header.DescribeStreamsResponse;
import header.ListRangesRequest;
import header.ListRangesRequestT;
import header.ListRangesResponse;
import header.ListRangesResultT;
import header.RangeCriteriaT;
import header.RangeIdT;
import header.SealRangesRequestT;
import header.SealRangesResponse;
import header.SealRangesResultT;
import header.StreamT;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import models.OperationCode;
import models.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static header.ErrorCode.OK;

public class ResourceManagerImpl implements ResourceManager {
    private static final Logger log = LoggerFactory.getLogger(ResourceManagerImpl.class);
    private final NettyClient nettyClient;

    public ResourceManagerImpl(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

    @Override
    public CompletableFuture<List<ListRangesResultT>> listRanges(List<RangeCriteriaT> rangeCriteriaList,
        Duration timeout) {
        Preconditions.checkArgument(rangeCriteriaList != null && rangeCriteriaList.size() > 0,
            "Invalid range criteria list since no range criteria was found.");

        ListRangesRequestT listRangesRequestT = new ListRangesRequestT();
        listRangesRequestT.setRangeCriteria(rangeCriteriaList.toArray(new RangeCriteriaT[0]));
        listRangesRequestT.setTimeoutMs((int) timeout.toMillis());
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int listRangesRequestOffset = ListRangesRequest.pack(builder, listRangesRequestT);
        builder.finish(listRangesRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.LIST_RANGES, builder.dataBuffer());
        return nettyClient.invokeAsync(sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                ListRangesResponse response = ListRangesResponse.getRootAsListRangesResponse(responseFrame.getHeader());
                Address updatePmAddress = PmUtil.extractNewPmAddress(response.status());
                // need to connect to new Pm primary node.
                if (updatePmAddress != null) {
                    nettyClient.updatePmAddress(updatePmAddress);
                    return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame2 -> extractResponse(header.ListRangesResponse.getRootAsListRangesResponse(responseFrame2.getHeader())));
                }
                return extractResponse(response);
            });
    }

    @Override
    public CompletableFuture<Byte> pingPong(Duration timeout) {
        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.PING, null);
        return nettyClient.invokeAsync(sbpFrame, timeout)
            .thenCompose(responseFrame -> CompletableFuture.completedFuture(responseFrame.getFlag()));
    }

    @Override
    public CompletableFuture<List<SealRangesResultT>> sealRanges(List<RangeIdT> rangeIdList, Duration timeout) {
        Preconditions.checkArgument(rangeIdList != null && rangeIdList.size() > 0, "Invalid range id list since no range id was found.");

        SealRangesRequestT sealRangesRequestT = new SealRangesRequestT();
        sealRangesRequestT.setRanges(rangeIdList.toArray(new RangeIdT[0]));
        sealRangesRequestT.setTimeoutMs((int) timeout.toMillis());
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int sealRangesRequestOffset = header.SealRangesRequest.pack(builder, sealRangesRequestT);
        builder.finish(sealRangesRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.SEAL_RANGES, builder.dataBuffer());
        return nettyClient.invokeAsync(sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                header.SealRangesResponse response = header.SealRangesResponse.getRootAsSealRangesResponse(responseFrame.getHeader());
                Address updatePmAddress = PmUtil.extractNewPmAddress(response.status());
                // need to connect to new Pm primary node.
                if (updatePmAddress != null) {
                    nettyClient.updatePmAddress(updatePmAddress);
                    return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame2 -> extractResponse(header.SealRangesResponse.getRootAsSealRangesResponse(responseFrame2.getHeader())));
                }
                return extractResponse(response);
            });
    }

    @Override
    public CompletableFuture<List<DescribeRangeResultT>> describeRanges(Address dataNodeAddress, List<RangeIdT> rangeIdList, Duration timeout) {
        Preconditions.checkArgument(rangeIdList != null && rangeIdList.size() > 0, "Invalid range id list since no range id was found.");

        DescribeRangesRequestT describeRangesRequestT = new DescribeRangesRequestT();
        describeRangesRequestT.setRanges(rangeIdList.toArray(new RangeIdT[0]));
        describeRangesRequestT.setTimeoutMs((int) timeout.toMillis());
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int describeRangesRequestOffset = DescribeRangesRequest.pack(builder, describeRangesRequestT);
        builder.finish(describeRangesRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.DESCRIBE_RANGES, builder.dataBuffer());
        return nettyClient.invokeAsync(dataNodeAddress, sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                DescribeRangesResponse response = DescribeRangesResponse.getRootAsDescribeRangesResponse(responseFrame.getHeader());
                return extractResponse(response);
            });
    }

    @Override
    public CompletableFuture<List<DescribeStreamResultT>> describeStreams(List<Long> streamIdList, Duration timeout) {
        Preconditions.checkArgument(streamIdList != null && streamIdList.size() > 0, "Invalid stream id list since no stream id was found.");

        DescribeStreamsRequestT describeStreamsRequestT = new DescribeStreamsRequestT();
        describeStreamsRequestT.setStreamIds(streamIdList.stream().mapToLong(Long::longValue).toArray());
        describeStreamsRequestT.setTimeoutMs((int) timeout.toMillis());
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int describeStreamsRequestOffset = DescribeStreamsRequest.pack(builder, describeStreamsRequestT);
        builder.finish(describeStreamsRequestOffset);

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.DESCRIBE_STREAMS, builder.dataBuffer());
        return nettyClient.invokeAsync(sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                DescribeStreamsResponse response = DescribeStreamsResponse.getRootAsDescribeStreamsResponse(responseFrame.getHeader());
                Address updatePmAddress = PmUtil.extractNewPmAddress(response.status());
                // need to connect to new Pm primary node.
                if (updatePmAddress != null) {
                    nettyClient.updatePmAddress(updatePmAddress);
                    return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame2 -> extractResponse(header.DescribeStreamsResponse.getRootAsDescribeStreamsResponse(responseFrame2.getHeader())));
                }
                return extractResponse(response);
            });
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

        SbpFrame sbpFrame = ProtocolUtil.constructRequestSbpFrame(OperationCode.CREATE_STREAMS, builder.dataBuffer());
        return nettyClient.invokeAsync(sbpFrame, timeout)
            .thenCompose(responseFrame -> {
                CreateStreamsResponse response = CreateStreamsResponse.getRootAsCreateStreamsResponse(responseFrame.getHeader());
                Address updatePmAddress = PmUtil.extractNewPmAddress(response.status());
                // need to connect to new Pm primary node.
                if (updatePmAddress != null) {
                    nettyClient.updatePmAddress(updatePmAddress);
                    return nettyClient.invokeAsync(sbpFrame, timeout).thenCompose(responseFrame2 -> extractResponse(CreateStreamsResponse.getRootAsCreateStreamsResponse(responseFrame2.getHeader())));
                }
                return extractResponse(response);
            });
    }

    /**
     * Extract response from ListRangesResponse.
     *
     * @param response ListRangesResponse
     * @return CompletableFuture<List < ListRangesResultT>>
     */
    private CompletableFuture<List<ListRangesResultT>> extractResponse(ListRangesResponse response) {
        CompletableFuture<List<ListRangesResultT>> completableFuture = new CompletableFuture<>();
        if (response.status().code() != OK) {
            completableFuture.completeExceptionally(new ClientException("List ranges failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return completableFuture;
        }
        completableFuture.complete(Arrays.asList(response.unpack().getListResponses()));
        return completableFuture;
    }

    /**
     * Extract response from SealRangesResponse.
     *
     * @param response SealRangesResponse
     * @return CompletableFuture<List < SealRangesResultT>>
     */
    private CompletableFuture<List<SealRangesResultT>> extractResponse(SealRangesResponse response) {
        CompletableFuture<List<SealRangesResultT>> completableFuture = new CompletableFuture<>();
        if (response.status().code() != OK) {
            completableFuture.completeExceptionally(new ClientException("Seal ranges failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return completableFuture;
        }
        completableFuture.complete(Arrays.asList(response.unpack().getSealResponses()));
        return completableFuture;
    }

    /**
     * Extract response from DescribeRangesResponse.
     *
     * @param response DescribeRangesResponse
     * @return CompletableFuture<List < DescribeRangeResultT>>
     */
    private CompletableFuture<List<DescribeRangeResultT>> extractResponse(DescribeRangesResponse response) {
        CompletableFuture<List<DescribeRangeResultT>> completableFuture = new CompletableFuture<>();
        if (response.status().code() != OK) {
            completableFuture.completeExceptionally(new ClientException("Describe ranges failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return completableFuture;
        }
        completableFuture.complete(Arrays.asList(response.unpack().getDescribeResponses()));
        return completableFuture;
    }

    /**
     * Extract response from CreateStreamsResponse.
     *
     * @param response CreateStreamsResponse
     * @return CompletableFuture<List < CreateStreamResultT>>
     */
    private CompletableFuture<List<CreateStreamResultT>> extractResponse(CreateStreamsResponse response) {
        CompletableFuture<List<CreateStreamResultT>> completableFuture = new CompletableFuture<>();
        if (response.status().code() != OK) {
            completableFuture.completeExceptionally(new ClientException("Create streams failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return completableFuture;
        }
        completableFuture.complete(Arrays.asList(response.unpack().getCreateResponses()));
        return completableFuture;
    }

    /**
     * Extract response from DescribeStreamsResponse.
     *
     * @param response DescribeStreamsResponse
     * @return CompletableFuture<List < DescribeStreamResultT>>
     */
    private CompletableFuture<List<DescribeStreamResultT>> extractResponse(DescribeStreamsResponse response) {
        CompletableFuture<List<DescribeStreamResultT>> completableFuture = new CompletableFuture<>();
        if (response.status().code() != OK) {
            completableFuture.completeExceptionally(new ClientException("Describe streams failed with code " + response.status().code() + ", msg: " + response.status().message()));
            return completableFuture;
        }
        completableFuture.complete(Arrays.asList(response.unpack().getDescribeResponses()));
        return completableFuture;
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
            appendInfoT.setStreamId(streamId);
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
}
