package models;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.RecordBatchMeta;
import records.RecordBatchMetaT;

import static org.junit.jupiter.api.Assertions.*;

class RecordBatchTest {

    @Test
    void testEncodeAndDecode() {
        long streamId = 1L;
        short flags = (short) 2;
        long baseOffset = 3L;
        int lastOffsetDelta = 4;
        long baseTimestamp = 5L;

        RecordBatchMetaT metaT = new RecordBatchMetaT();
        metaT.setStreamId(streamId);
        metaT.setFlags(flags);
        metaT.setBaseOffset(baseOffset);
        metaT.setLastOffsetDelta(lastOffsetDelta);
        metaT.setBaseTimestamp(baseTimestamp);

        byte magic = 6;
        int batchBaseOffset = 7;
        RecordBatch batch = new RecordBatch(magic, batchBaseOffset, metaT, null);
        ByteBuffer encode = batch.encode();
        RecordBatch decodedBatch = new RecordBatch(encode);

        Assertions.assertEquals(magic, decodedBatch.getMagic());
        Assertions.assertEquals(batchBaseOffset, decodedBatch.getBaseOffset());

        RecordBatchMetaT decodedBatchMeta = decodedBatch.getBatchMeta();
        Assertions.assertEquals(streamId, decodedBatchMeta.getStreamId());
        Assertions.assertEquals(flags, decodedBatchMeta.getFlags());
        Assertions.assertEquals(baseOffset, decodedBatchMeta.getBaseOffset());
        Assertions.assertEquals(lastOffsetDelta, decodedBatchMeta.getLastOffsetDelta());
        Assertions.assertEquals(baseTimestamp, decodedBatchMeta.getBaseTimestamp());

        Assertions.assertEquals(decodedBatch.getRecords().size(), 0);
    }
}