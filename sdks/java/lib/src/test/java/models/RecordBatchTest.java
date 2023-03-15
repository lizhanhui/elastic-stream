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

        FlatBufferBuilder builder = new FlatBufferBuilder();
        int meta = records.RecordBatchMeta.pack(builder, metaT);
        builder.finish(meta);
        ByteBuffer batchMetaBuffer = builder.dataBuffer();

        byte magic = 6;
        int batchBaseOffset = 7;
        RecordBatch batch = new RecordBatch(magic, batchBaseOffset, batchMetaBuffer, null);
        ByteBuffer encode = batch.encode();
        RecordBatch decodedBatch = new RecordBatch(encode);

        Assertions.assertEquals(magic, decodedBatch.getMagic());
        Assertions.assertEquals(batchBaseOffset, decodedBatch.getBaseOffset());

        RecordBatchMeta decodedBatchMeta = RecordBatchMeta.getRootAsRecordBatchMeta(decodedBatch.getBatchMeta());
        Assertions.assertEquals(streamId, decodedBatchMeta.streamId());
        Assertions.assertEquals(flags, decodedBatchMeta.flags());
        Assertions.assertEquals(baseOffset, decodedBatchMeta.baseOffset());
        Assertions.assertEquals(lastOffsetDelta, decodedBatchMeta.lastOffsetDelta());
        Assertions.assertEquals(baseTimestamp, decodedBatchMeta.baseTimestamp());

        Assertions.assertEquals(decodedBatch.getRecords().size(), 0);
    }
}