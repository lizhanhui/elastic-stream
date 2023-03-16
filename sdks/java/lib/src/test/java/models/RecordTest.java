package models;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.RecordMeta;
import records.RecordMetaT;

class RecordTest {

    @Test
    void testEncodeAndDecode() {
        int offsetDelta = 1;
        int timestampDelta = 2;
        RecordMetaT metaT = new RecordMetaT();
        metaT.setOffsetDelta(offsetDelta);
        metaT.setTimestampDelta(timestampDelta);

        FlatBufferBuilder builder = new FlatBufferBuilder();
        int meta = RecordMeta.pack(builder, metaT);
        builder.finish(meta);
        ByteBuffer metaByteBuffer = builder.dataBuffer();

        byte[] body = new byte[] {0, 1, 3};
        Record record = new Record(metaByteBuffer.duplicate(), ByteBuffer.wrap(body));

        ByteBuffer encoded = record.encode();
        List<Record> decodedList = Record.decode(encoded);

        Assertions.assertEquals(1, decodedList.size());
        RecordMeta decodedMeta = RecordMeta.getRootAsRecordMeta(decodedList.get(0).getMeta());
        Assertions.assertEquals(offsetDelta, decodedMeta.offsetDelta());
        Assertions.assertEquals(timestampDelta, decodedMeta.timestampDelta());

        Assertions.assertEquals(body.length, decodedList.get(0).getBody().remaining());
        for (byte b : body) {
            Assertions.assertEquals(b, decodedList.get(0).getBody().get());
        }

    }
}