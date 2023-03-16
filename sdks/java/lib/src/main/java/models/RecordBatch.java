package models;

import java.nio.ByteBuffer;
import java.util.List;

public class RecordBatch {
    private static final int MIN_LENGTH = 9;
    private byte magic;
    private int baseOffset;
    private ByteBuffer batchMeta;
    private List<Record> records;

    public RecordBatch(byte magic, int baseOffset, ByteBuffer batchMeta, List<Record> records) {
        this.magic = magic;
        this.baseOffset = baseOffset;
        this.batchMeta = batchMeta;
        this.records = records;
    }

    public RecordBatch(ByteBuffer buffer) {
        assert buffer != null && buffer.remaining() >= MIN_LENGTH;
        this.magic = buffer.get();
        this.baseOffset = buffer.getInt();
        int metaLength = buffer.getInt();

        assert buffer.remaining() >= metaLength;
        byte[] batchMetaBytes = new byte[metaLength];
        buffer.get(batchMetaBytes);
        this.batchMeta = ByteBuffer.wrap(batchMetaBytes);
        this.records = Record.decode(buffer);
    }

    public ByteBuffer encode() {
        ByteBuffer resultBuffer = ByteBuffer.allocate(getEncodeLength())
            .put(magic)
            .putInt(baseOffset)
            .putInt(batchMeta.remaining())
            .put(batchMeta.duplicate());
        if (records != null) {
            for (Record record : records) {
                resultBuffer.put(record.encode());
            }
        }

        resultBuffer.flip();
        return resultBuffer;
    }

    public int getEncodeLength() {
        int totalLength = MIN_LENGTH + batchMeta.remaining();
        if (records == null) {
            return totalLength;
        }

        for (Record record: records) {
            totalLength += record.getEncodeLength();
        }
        return totalLength;
    }

    public byte getMagic() {
        return magic;
    }

    public int getBaseOffset() {
        return baseOffset;
    }

    public ByteBuffer getBatchMeta() {
        return batchMeta;
    }

    public List<Record> getRecords() {
        return records;
    }
}
