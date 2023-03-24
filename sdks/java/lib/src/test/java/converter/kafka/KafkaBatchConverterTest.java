package converter.kafka;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sdk.elastic.storage.converter.kafka.KafkaBatchConverter;

class KafkaBatchConverterTest {
    @Test
    public void testConversion() {
        MemoryRecords records = generateMemoryRecords();
        List<sdk.elastic.storage.models.RecordBatch> batchList = KafkaBatchConverter.toRecordBatch(records, 1L, true);
        MemoryRecords decodedRecords = KafkaBatchConverter.toKafkaMemoryRecords(batchList, true);
        Assertions.assertEquals(records.buffer(), decodedRecords.buffer());
    }

    private static MemoryRecords generateMemoryRecords() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = Integer.MAX_VALUE - 1;
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
            TimestampType.CREATE_TIME, 1234567L, RecordBatch.NO_TIMESTAMP, pid, epoch, baseSequence)) {
            builder.appendWithOffset(1234567, 1L, "a".getBytes(), "v".getBytes());
            builder.appendWithOffset(1234568, 2L, "b".getBytes(), "v".getBytes());
            builder.appendWithOffset(1234569, 3L, "c".getBytes(), "v".getBytes());

            return builder.build();
        }
    }

}