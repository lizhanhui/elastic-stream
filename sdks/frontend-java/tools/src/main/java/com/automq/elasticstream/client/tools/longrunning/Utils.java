package com.automq.elasticstream.client.tools.longrunning;

import org.apache.log4j.Logger;
import com.automq.elasticstream.client.DefaultRecordBatch;
import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.Client;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Utils {

    private static Logger log = Logger.getLogger(Utils.class.getClass());

    public static byte[] generateRandomByteArray(int min, int max) {
        int length = getRandomInt(min, max);
        byte[] byteArray = new byte[length];
        new Random().nextBytes(byteArray);
        return byteArray;
    }

    public static int getRandomInt(int min, int max) {
        Random random = new Random();
        return min + random.nextInt(max - min);
    }

    public static long calculateCRC32(byte[] byteArray) {
        CRC32 crc32 = new CRC32();
        crc32.update(byteArray);
        return crc32.getValue();
    }

    public static ByteBuffer getRecord(long seq, int min, int max) {
        byte[] payload = generateRandomByteArray(min, max);
        long crc32 = calculateCRC32(payload);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES +
                payload.length);
        buffer.putLong(seq);
        buffer.putLong(crc32);
        buffer.put(payload);
        buffer.flip();
        return buffer;
    }

    public static Boolean checkRecord(long seq0, ByteBuffer buffer) {
        long seq = buffer.getLong();
        if (seq != seq0) {
            log.error("Out of order, expect seq: " + seq0 + ", but get: " + seq);
            return false;
        }
        long crc = buffer.getLong();
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        long crc0 = calculateCRC32(payload);
        if (crc0 != crc) {
            log.error("Payload corrupted, expect crc32: " + crc + ", but get: " + crc0);
            return false;
        }
        return true;
    }

    public static boolean appendRecords(Stream stream, long startIndex, long count, int batchSize) {
        CountDownLatch latch = new CountDownLatch((int) count);
        for (long i = startIndex; i < startIndex + count; i++) {
            long index = i;
            byte[] payload = String.format("hello world %03d",
                    i).getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            CompletableFuture<AppendResult> cf = stream
                    .append(new DefaultRecordBatch(batchSize, 0, Collections.emptyMap(),
                            buffer));
            cf.whenComplete((rst, ex) -> {
                if (ex == null) {
                    long offset = rst.baseOffset();
                    if (index * batchSize == offset) {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean fetchRecords(Stream stream, long startIndex, long count, int batchSize) {
        for (long i = startIndex; i < startIndex + count; i++) {
            FetchResult fetchResult;
            try {
                fetchResult = stream.fetch(i * batchSize, i * batchSize + batchSize, Integer.MAX_VALUE).get();
            } catch (InterruptedException | ExecutionException e) {
                return false;
            }
            if (1 != fetchResult.recordBatchList().size()) {
                return false;
            }
            RecordBatchWithContext recordBatch = fetchResult.recordBatchList().get(0);
            if (i * batchSize != recordBatch.baseOffset() || i * batchSize + batchSize != recordBatch.lastOffset()) {
                return false;
            }
            byte[] rawPayload = new byte[recordBatch.rawPayload().remaining()];
            recordBatch.rawPayload().get(rawPayload);
            String payloadStr = new String(rawPayload, StandardCharsets.UTF_8);
            if (!String.format("hello world %03d", i).equals(payloadStr)) {
                return false;
            }
            fetchResult.free();
        }
        return true;
    }

    public static Stream openStream(Client client, long streamId, OpenStreamOptions options) {
        try {
            Stream stream = client.streamClient().openStream(streamId, options).get();
            return stream;
        } catch (InterruptedException | ExecutionException e) {
            return null;
        }
    }
}
