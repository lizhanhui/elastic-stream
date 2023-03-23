package converter.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import models.Record;
import models.RecordBatch;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;

/**
 * Kafka batch converter.
 * Note that the record and the batch are equivalent between Kafka and this SDK's model.
 */
public class KafkaBatchConverter {
    private static final String KAFKA_BATCH_ATTRIBUTE_PREFIX = "__KAFKA_";
    private static final String MAGIC_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "m";
    private static final String PARTITION_LEADER_EPOCH_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "pLE";
    private static final String PRODUCER_ID_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "pI";
    private static final String PRODUCER_EPOCH_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "pE";
    private static final String BASE_SEQUENCE_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "bS";
    private static final String IS_CONTROL_BATCH_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "iC";
    private static final String IS_TRANSACTIONAL_BATCH_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "iT";

    private static final String COMPRESSION_TYPE_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "cT";
    private static final String TIMESTAMP_TYPE_IN_PROPERTIES = KAFKA_BATCH_ATTRIBUTE_PREFIX + "tT";
    private static final String KEY_IN_PROPERTIES = "__key";

    /**
     * Convert a Kafka MemoryRecords to a RecordBatch list
     *
     * @param memoryRecords       Kafka MemoryRecords
     * @param streamId            Stream ID
     * @param storeBatchAttribute Whether to store batch attributes
     * @return RecordBatch list
     */
    public static List<RecordBatch> toRecordBatch(MemoryRecords memoryRecords, long streamId,
        boolean storeBatchAttribute) {
        List<RecordBatch> batchList = new ArrayList<>();
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            batchList.add(toRecordBatch(batch, streamId, storeBatchAttribute));
        }
        return batchList;
    }

    /**
     * Convert a Kafka RecordBatch to a RecordBatch
     *
     * @param kafkaRecordBatch    Kafka RecordBatch
     * @param streamId            Stream ID
     * @param storeBatchAttribute Whether to store batch attributes
     * @return RecordBatch
     */
    public static RecordBatch toRecordBatch(org.apache.kafka.common.record.RecordBatch kafkaRecordBatch, long streamId,
        boolean storeBatchAttribute) {
        List<Record> recordList = new ArrayList<>();
        Map<String, String> batchAttributes = new HashMap<>();

        if (storeBatchAttribute) {
            batchAttributes.put(MAGIC_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.magic()));
            batchAttributes.put(PARTITION_LEADER_EPOCH_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.partitionLeaderEpoch()));
            batchAttributes.put(PRODUCER_ID_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.producerId()));
            batchAttributes.put(PRODUCER_EPOCH_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.producerEpoch()));
            batchAttributes.put(BASE_SEQUENCE_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.baseSequence()));
            if (kafkaRecordBatch.isControlBatch()) {
                batchAttributes.put(IS_CONTROL_BATCH_IN_PROPERTIES, "");
            }
            if (kafkaRecordBatch.isTransactional()) {
                batchAttributes.put(IS_TRANSACTIONAL_BATCH_IN_PROPERTIES, "");
            }
            batchAttributes.put(COMPRESSION_TYPE_IN_PROPERTIES, String.valueOf(kafkaRecordBatch.compressionType().id));
            batchAttributes.put(TIMESTAMP_TYPE_IN_PROPERTIES, kafkaRecordBatch.timestampType().name);
        }

        for (org.apache.kafka.common.record.Record kafkaRecord : kafkaRecordBatch) {
            Map<String, String> properties = new HashMap<>();
            for (Header header : kafkaRecord.headers()) {
                properties.put(header.key(), new String(header.value(), StandardCharsets.ISO_8859_1));
            }

            // If the record contains a key, store it in properties.
            if (kafkaRecord.keySize() > 0) {
                byte[] keyBytes = new byte[kafkaRecord.keySize()];
                kafkaRecord.key().get(keyBytes);
                properties.put(KEY_IN_PROPERTIES, new String(keyBytes, StandardCharsets.ISO_8859_1));
            }
            // SequenceId may also need to be stored in properties. Ignore it for now. Recover it by baseSequence + index.

            recordList.add(new Record(streamId, kafkaRecord.offset(), kafkaRecord.timestamp(), null, properties, kafkaRecord.value()));
        }

        // add the last batch's attributes to the first record's properties.
        if (storeBatchAttribute && !recordList.isEmpty() && !batchAttributes.isEmpty()) {
            recordList.get(0).getProperties().putAll(batchAttributes);
        }

        return new RecordBatch(streamId, recordList);
    }

    /**
     * Convert RecordBatches to a single Kafka MemoryRecords
     *
     * @param recordBatchList       RecordBatch list
     * @param recoverBatchAttribute Whether to recover batch attributes
     * @return Kafka MemoryRecords
     */
    public static MemoryRecords toKafkaMemoryRecords(List<RecordBatch> recordBatchList, boolean recoverBatchAttribute) {
        if (recordBatchList == null || recordBatchList.isEmpty()) {
            return MemoryRecords.EMPTY;
        }

        int resultBufferSize = 0;
        List<MemoryRecords> memoryRecordsList = new ArrayList<>(recordBatchList.size());

        for (RecordBatch recordBatch : recordBatchList) {
            MemoryRecords records = toKafkaMemoryRecords(recordBatch, recoverBatchAttribute);
            memoryRecordsList.add(records);
            resultBufferSize += records.sizeInBytes();
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(resultBufferSize);
        for (MemoryRecords memoryRecords : memoryRecordsList) {
            resultBuffer.put(memoryRecords.buffer());
        }
        resultBuffer.flip();
        return MemoryRecords.readableRecords(resultBuffer);
    }

    /**
     * Convert a RecordBatch to a Kafka MemoryRecords.
     * Note that there is actually only one batch in the returned MemoryRecords.
     *
     * @param recordBatch           RecordBatch
     * @param recoverBatchAttribute Whether to recover batch attributes
     * @return Kafka MemoryRecords
     */
    public static MemoryRecords toKafkaMemoryRecords(RecordBatch recordBatch, boolean recoverBatchAttribute) {
        byte magic = org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
        long producerId = -1;
        short producerEpoch = -1;
        int partitionLeaderEpoch = -1;
        int baseSequence = -1;
        boolean isTransactional = false;
        boolean isControlBatch = false;
        CompressionType compressionType = CompressionType.NONE;
        TimestampType timestampType = TimestampType.CREATE_TIME;

        Map<String, String> baseProperties = recordBatch.getRecords().get(0).getProperties();
        if (recoverBatchAttribute) {
            String magicStr = baseProperties.remove(MAGIC_IN_PROPERTIES);
            if (magicStr != null) {
                magic = Byte.parseByte(magicStr);
            }
            String partitionLeaderEpochStr = baseProperties.remove(PARTITION_LEADER_EPOCH_IN_PROPERTIES);
            if (partitionLeaderEpochStr != null) {
                partitionLeaderEpoch = Integer.parseInt(partitionLeaderEpochStr);
            }
            String producerIdStr = baseProperties.remove(PRODUCER_ID_IN_PROPERTIES);
            if (producerIdStr != null) {
                producerId = Long.parseLong(producerIdStr);
            }
            String producerEpochStr = baseProperties.remove(PRODUCER_EPOCH_IN_PROPERTIES);
            if (producerEpochStr != null) {
                producerEpoch = Short.parseShort(producerEpochStr);
            }
            String baseSequenceStr = baseProperties.remove(BASE_SEQUENCE_IN_PROPERTIES);
            if (baseSequenceStr != null) {
                baseSequence = Integer.parseInt(baseSequenceStr);
            }
            isTransactional = baseProperties.remove(IS_TRANSACTIONAL_BATCH_IN_PROPERTIES) != null;
            isControlBatch = baseProperties.remove(IS_CONTROL_BATCH_IN_PROPERTIES) != null;
            String compressionTypeStr = baseProperties.remove(COMPRESSION_TYPE_IN_PROPERTIES);
            if (compressionTypeStr != null) {
                compressionType = CompressionType.forId(Integer.parseInt(compressionTypeStr));
            }
            String timestampTypeStr = baseProperties.remove(TIMESTAMP_TYPE_IN_PROPERTIES);
            if (timestampTypeStr != null) {
                timestampType = TimestampType.forName(timestampTypeStr);
            }
        }

        long firstOffset = recordBatch.getBaseOffset();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compressionType,
            timestampType, firstOffset, org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP, producerId, producerEpoch, baseSequence,
            isTransactional, isControlBatch, partitionLeaderEpoch)) {
            for (Record record : recordBatch.getRecords()) {
                List<Header> headers = new ArrayList<>();
                String key = record.getProperties().remove(KEY_IN_PROPERTIES);
                ByteBuffer keyByteBuffer = key == null ? null : ByteBuffer.wrap(key.getBytes(StandardCharsets.ISO_8859_1));
                record.getProperties().forEach((k, v) -> headers.add(new RecordHeader(k, v.getBytes(StandardCharsets.ISO_8859_1))));
                builder.appendWithOffset(record.getOffset(), record.getTimestamp(), keyByteBuffer, record.getBody(), headers.toArray(new Header[0]));
            }
            return builder.build();
        }
    }
}
