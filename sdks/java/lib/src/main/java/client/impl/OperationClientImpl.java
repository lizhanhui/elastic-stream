package client.impl;

import apis.OperationClient;
import com.google.common.base.Preconditions;
import header.AppendResultT;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import models.RecordBatch;

public class OperationClientImpl implements OperationClient {
    @Override
    public CompletableFuture<AppendResultT> appendBatch(RecordBatch recordBatch, Duration timeout) {
        Preconditions.checkArgument(recordBatch != null && recordBatch.getRecords().size() > 0, "Invalid recordBatch since no records were found.");

        long beginStartTime = System.currentTimeMillis();

//
//
//        RecordBatchMeta batchMeta = RecordBatchMeta.getRootAsRecordBatchMeta(recordBatch.getBatchMeta().duplicate());
//        long streamId = batchMeta.streamId();
//        Range range = getLastRange(streamId);
//        Preconditions.checkNotNull(range, "Failed to get last range of stream " + streamId);

        return null;




//        long costTime = System.currentTimeMillis() - beginStartTime;
//        if (timeoutMillis < costTime) {
//            throw new RemotingTimeoutException("Sync call timeout before sending batch to servers");
//        }
//
//        Map<Long, AppendRequest> streamIdToAppendRequestMap = generateAppendRequest(recordBatches, (int) (timeoutMillis - costTime));
//        streamIdToAppendRequestMap.forEach((streamId, appendRequest) -> {
//            Range range = getLastRange(streamId);
//            streamRangeCache.update(streamId, appendRequest.getRange());
//        });
//
//        AppendRequestT appendRequestT = new AppendRequestT();
//        appendRequestT.setTimeoutMs((int) (timeoutMillis - costTime));
//
////        ByteBuffer header = HeaderConverter.appendBatchRequestHeader(recordBatch, timeoutMillis);
//        SbpFrame sbpFrame = constructRequestSbpFrame(OperationCode.APPEND, (int) streamId, null, new ByteBuffer[]{recordBatch.encode()});
//
//        try {
//            SbpFrame response = (SbpFrame) nettyClient.invokeSync(sbpFrame, timeoutMillis - costTime);
//            return response.
//        } catch (InterruptedException | RemotingSendRequestException | RemotingConnectException |
//                 RemotingTimeoutException e) {
//            log.error("Failed to append batch to server. errorMessage: {}", e.getMessage());
//            throw e;
//        }
    }

    @Override
    public void close() throws IOException {

    }
}
