package models;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import sdk.elastic.storage.models.Headers;
import sdk.elastic.storage.models.Record;
import sdk.elastic.storage.models.RecordBatch;

import static sdk.elastic.storage.models.HeaderKey.CreatedAt;
import static sdk.elastic.storage.models.HeaderKey.Tag;

public class RecordBatchesGenerator {

    public static RecordBatch generateOneRecordBatch(long streamId) {
        List<Record> recordList = generateRecordList(streamId);
        return new RecordBatch(streamId, recordList);
    }

    public static List<Record> generateRecordList(long streamId) {
        List<Record> recordList = new ArrayList<>(2);

        Headers headers = new Headers();
        headers.addHeader(CreatedAt, "someTime");
        Map<String, String> properties = new HashMap<>();
        properties.put("propertyA", "valueA");
        recordList.add(new Record(streamId, 2L, 13L, headers, properties, ByteBuffer.wrap(new byte[] {0, 1, 3})));

        Headers headers2 = new Headers();
        headers2.addHeader(CreatedAt, "someTime2");
        headers2.addHeader(Tag, "onlyTest");
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("propertyB", "valueB");
        properties2.put("propertyC", "valueC");
        recordList.add(new Record(streamId, 10L, 11L, headers2, properties2, ByteBuffer.wrap(new byte[] {3, 6, 8, 10})));

        return recordList;
    }
}
