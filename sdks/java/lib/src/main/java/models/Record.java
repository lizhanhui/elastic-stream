package models;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import records.RecordMeta;
import records.RecordMetaT;

public class Record {
    private static final int MIN_LENGTH = 8;
    private ByteBuffer meta;
    private ByteBuffer body;

    public Record(RecordMetaT meta, ByteBuffer body) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int pack = RecordMeta.pack(builder, meta);
        builder.finish(pack);
        this.meta = builder.dataBuffer();
        this.body = body;
    }

    private Record(ByteBuffer buffer) {
        int metaLength = buffer.getInt();
        int bodyLength = buffer.getInt();

        assert buffer.remaining() >= metaLength + bodyLength;
        byte[] metaBytes = new byte[metaLength];
        buffer.get(metaBytes);
        byte[] bodyBytes = new byte[bodyLength];
        buffer.get(bodyBytes);
        this.meta = ByteBuffer.wrap(metaBytes);
        this.body = ByteBuffer.wrap(bodyBytes);
    }

    public static List<Record> decode(ByteBuffer buffer) {
        assert buffer != null;
        List<Record> records = new ArrayList<>();
        while (buffer.remaining() >= MIN_LENGTH) {
            records.add(new Record(buffer));
        }
        return records;
    }

    public RecordMetaT getMeta() {
        return RecordMeta.getRootAsRecordMeta(meta).unpack();
    }

    public ByteBuffer getBody() {
        return body;
    }

    public ByteBuffer encode() {
        ByteBuffer resultBuffer = ByteBuffer.allocate(getEncodeLength())
            .putInt(meta.remaining())
            .putInt(body.remaining())
            .put(meta.duplicate())
            .put(body.duplicate());
        resultBuffer.flip();
        return resultBuffer;
    }

    public int getEncodeLength() {
        return meta.remaining() + body.remaining() + MIN_LENGTH;
    }
}
