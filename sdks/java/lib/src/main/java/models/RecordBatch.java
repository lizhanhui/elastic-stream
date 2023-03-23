package models;

import client.common.ProtocolUtil;
import com.google.common.base.Preconditions;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import records.RecordBatchMeta;
import records.RecordBatchMetaT;
import records.RecordMeta;
import records.RecordMetaT;

/**
 * A record batch wraps a list of records with the same stream id.
 * Note that data nodes store records with a whole {@link RecordBatch} instead of a single {@link Record}.
 * Besides, offsets of records in a record batch are in ascending order. However, timestamps of records in a record batch may be not.
 */
public class RecordBatch {
    private static final int MIN_LENGTH = 13;
    private static final byte DEFAULT_MAGIC = 0x22;
    private static final short DEFAULT_FLAG = 0;
    private static final int MAX_OFFSET_DELTA = Integer.MAX_VALUE;
    private final byte magic;
    private final RecordBatchMetaT batchMeta;
    private final List<Record> records;

    public RecordBatch(long streamId, short flags, byte magic, List<Record> records) {
        Preconditions.checkArgument(records != null && records.size() > 0, "records should not be empty");

        this.magic = magic;
        this.records = records;

        long baseOffset = records.get(0).getOffset();
        long baseTimestamp = records.get(0).getTimestamp();
        this.batchMeta = new RecordBatchMetaT();
        batchMeta.setStreamId(streamId);
        batchMeta.setBaseOffset(baseOffset);
        batchMeta.setBaseTimestamp(baseTimestamp);
        batchMeta.setFlags(flags);

        Preconditions.checkArgument(records.get(records.size() - 1).getOffset() - baseOffset <= MAX_OFFSET_DELTA,
            "offset delta should not exceed " + MAX_OFFSET_DELTA);
        batchMeta.setLastOffsetDelta((int) (records.get(records.size() - 1).getOffset() - baseOffset));
    }

    public RecordBatch(long streamId, short flags, List<Record> records) {
        this(streamId, flags, DEFAULT_MAGIC, records);
    }

    public RecordBatch(long streamId, List<Record> records) {
        this(streamId, DEFAULT_FLAG, records);
    }

    public RecordBatch(ByteBuffer buffer) {
        Preconditions.checkArgument(buffer != null && buffer.remaining() >= MIN_LENGTH, "buffer should contain at least " + MIN_LENGTH + " bytes");
        this.magic = buffer.get();
        long baseOffset = buffer.getLong();
        int metaLength = buffer.getInt();

        assert buffer.remaining() >= metaLength;
        byte[] batchMetaBytes = new byte[metaLength];
        buffer.get(batchMetaBytes);
        this.batchMeta = RecordBatchMeta.getRootAsRecordBatchMeta(ByteBuffer.wrap(batchMetaBytes)).unpack();

        // Clients have to accept the outside BaseOffset.
        this.batchMeta.setBaseOffset(baseOffset);
        this.records = extractRecordList(buffer);
    }

    /**
     * Decode a list of record batches from a byte buffer. Only the first ${length} bytes will be decoded.
     * If not enough bytes are available, an empty list will be returned.
     *
     * @param buffer the byte buffer containing the record batches
     * @param length the length of bytes in the buffer to be decoded
     * @return a list of record batches
     */
    public static List<RecordBatch> decode(ByteBuffer buffer, int length) {
        assert buffer != null && buffer.remaining() >= length;
        List<RecordBatch> batchList = new ArrayList<>();
        int oldRemaining = buffer.remaining();
        while (buffer.remaining() >= MIN_LENGTH && oldRemaining - buffer.remaining() <= length) {
            batchList.add(new RecordBatch(buffer));
        }
        return batchList;
    }

    /**
     * Encode to a byte buffer.
     *
     * @return a byte buffer, which is ready to be stored or sent.
     */
    public ByteBuffer encode() {
        int totalLength = MIN_LENGTH;
        List<ByteBuffer[]> recordsBufferList = new ArrayList<>(records.size());
        for (Record record : records) {
            RecordMetaT metaT = new RecordMetaT();
            assert record.getTimestamp() - batchMeta.getBaseTimestamp() <= MAX_OFFSET_DELTA && record.getTimestamp() - -batchMeta.getBaseTimestamp() >= Integer.MIN_VALUE;
            metaT.setTimestampDelta((int) (record.getTimestamp() - batchMeta.getBaseTimestamp()));
            metaT.setHeaders(record.getHeaders().toKeyValueTArray());
            metaT.setProperties(ProtocolUtil.map2KeyValueTList(record.getProperties()));
            metaT.setOffsetDelta((int) (record.getOffset() - batchMeta.getBaseOffset()));
            FlatBufferBuilder builder = new FlatBufferBuilder();
            int metaOffset = RecordMeta.pack(builder, metaT);
            builder.finish(metaOffset);

            // add meta and body
            recordsBufferList.add(new ByteBuffer[] {builder.dataBuffer(), record.getBody().duplicate()});
            totalLength += Record.ENCODED_MIN_LENGTH + builder.dataBuffer().remaining() + record.getBody().remaining();
        }

        FlatBufferBuilder builder = new FlatBufferBuilder();
        int metaOffset = RecordBatchMeta.pack(builder, batchMeta);
        builder.finish(metaOffset);
        totalLength += builder.dataBuffer().remaining();

        // Note that there are two BaseOffsets when encoding. One is kept in {@link RecordBatchMeta}, and the other is generated when encoding.
        // The data nodes may modify and only modify the BaseOffset outside the {@link RecordBatchMeta} when storing this batch.
        ByteBuffer resultBuffer = ByteBuffer.allocate(totalLength)
            .put(magic)
            .putLong(batchMeta.getBaseOffset())
            .putInt(builder.dataBuffer().remaining())
            .put(builder.dataBuffer());
        for (ByteBuffer[] bufferArray : recordsBufferList) {
            // put in MetaLength
            resultBuffer.putInt(bufferArray[0].remaining());
            // put in BodyLength
            resultBuffer.putInt(bufferArray[1].remaining());
            // put in RecordMeta
            resultBuffer.put(bufferArray[0]);
            // put in RecordBody
            resultBuffer.put(bufferArray[1]);
        }

        resultBuffer.flip();
        return resultBuffer;
    }

    public byte getMagic() {
        return magic;
    }

    public long getBaseOffset() {
        return batchMeta.getBaseOffset();
    }

    public RecordBatchMetaT getBatchMeta() {
        return batchMeta;
    }

    public List<Record> getRecords() {
        return records;
    }

    private List<Record> extractRecordList(ByteBuffer buffer) {
        assert buffer != null;

        int lastDelta = this.batchMeta.getLastOffsetDelta();
        List<Record> recordList = new ArrayList<>();
        while (buffer.remaining() >= Record.ENCODED_MIN_LENGTH) {
            Record record = new Record(buffer, this.batchMeta.getStreamId(), getBaseOffset(), this.batchMeta.getBaseTimestamp());
            recordList.add(record);
            // Meet the ending of this batch.
            if (record.getOffset() - getBaseOffset() >= lastDelta) {
                break;
            }
        }
        return recordList;
    }
}
