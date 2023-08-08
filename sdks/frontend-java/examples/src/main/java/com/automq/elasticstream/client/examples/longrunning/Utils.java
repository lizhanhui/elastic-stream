package com.automq.elasticstream.client.examples.longrunning;

import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.Random;

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
}
