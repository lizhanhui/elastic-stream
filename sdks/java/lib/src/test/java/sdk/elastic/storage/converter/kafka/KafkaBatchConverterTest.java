package sdk.elastic.storage.converter.kafka;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaBatchConverterTest {
    @Test
    public void testConversion() {
        List<MemoryRecords> toTestList = Arrays.asList(
            generateDataMemoryRecords(),
            generateLeaderChangeMemoryRecords(),
            generateEndTxnMarkerMemoryRecords(),
            generateSnapshotHeaderMemoryRecords(),
            generateSnapshotFooterMemoryRecords()
        );
        for (MemoryRecords records : toTestList) {
            List<sdk.elastic.storage.models.RecordBatch> batchList = KafkaBatchConverter.toRecordBatch(records, 1L);
            MemoryRecords decodedRecords = KafkaBatchConverter.toKafkaMemoryRecords(batchList);
            assertEquals(records.buffer(), decodedRecords.buffer());
        }
    }

    private static MemoryRecords generateSnapshotHeaderMemoryRecords() {
        SnapshotHeaderRecord headerRecord = new SnapshotHeaderRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
            .setLastContainedLogTimestamp(0);
        return MemoryRecords.withSnapshotHeaderRecord(
            1234567L,
            System.currentTimeMillis(),
            101,
            ByteBuffer.allocate(100),
            headerRecord
        );
    }

    private static MemoryRecords generateSnapshotFooterMemoryRecords() {
        SnapshotFooterRecord footerRecord = new SnapshotFooterRecord()
            .setVersion(ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION);
        return MemoryRecords.withSnapshotFooterRecord(
            1234567L,
            System.currentTimeMillis(),
            102,
            ByteBuffer.allocate(100),
            footerRecord
        );
    }

    private static MemoryRecords generateDataMemoryRecords() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = Integer.MAX_VALUE - 1;
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
            TimestampType.CREATE_TIME, 1234567L, org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP, pid, epoch, baseSequence)) {
            builder.appendWithOffset(1234567, 1L, "a".getBytes(), "v".getBytes());
            builder.appendWithOffset(1234568, 2L, "b".getBytes(), "v".getBytes());
            builder.appendWithOffset(1234569, 3L, "c".getBytes(), "v".getBytes());
            builder.appendWithOffset(1234570, 4L, null, "v".getBytes());
            builder.appendWithOffset(1234571, 5L, "e".getBytes(), null);

            return builder.build();
        }
    }

    private static MemoryRecords generateLeaderChangeMemoryRecords() {
        final int leaderId = 5;
        final int leaderEpoch = 20;
        final int voterId = 6;
        long initialOffset = 983L;

        LeaderChangeMessage leaderChangeMessage = new LeaderChangeMessage()
            .setLeaderId(leaderId)
            .setVoters(Collections.singletonList(
                new LeaderChangeMessage.Voter().setVoterId(voterId)));
        ByteBuffer buffer = ByteBuffer.allocate(256);
        return MemoryRecords.withLeaderChangeMessage(
            initialOffset,
            System.currentTimeMillis(),
            leaderEpoch,
            buffer,
            leaderChangeMessage
        );
    }

    private static MemoryRecords generateEndTxnMarkerMemoryRecords() {
        long producerId = 73;
        short producerEpoch = 13;
        long initialOffset = 983L;
        int coordinatorEpoch = 347;
        int partitionLeaderEpoch = 29;

        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        return MemoryRecords.withEndTransactionMarker(initialOffset, System.currentTimeMillis(),
            partitionLeaderEpoch, producerId, producerEpoch, marker);
    }

}