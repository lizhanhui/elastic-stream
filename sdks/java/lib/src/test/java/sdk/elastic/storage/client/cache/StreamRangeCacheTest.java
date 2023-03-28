package sdk.elastic.storage.client.cache;

import com.google.common.cache.CacheLoader;
import java.util.AbstractMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sdk.elastic.storage.flatc.header.DataNodeT;
import sdk.elastic.storage.flatc.header.RangeT;
import sdk.elastic.storage.flatc.header.ReplicaNodeT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamRangeCacheTest {
    private static ReplicaNodeT replicaNodeT;
    private static CacheLoader<Long, TreeMap<Long, RangeT>> emptyCacheLoader = new CacheLoader<Long, TreeMap<Long, RangeT>>() {
        @Override
        public TreeMap<Long, RangeT> load(Long key) throws Exception {
            return new TreeMap<>();
        }
    };

    @BeforeAll
    static void setUpAll() {
        DataNodeT dataNodeT = new DataNodeT();
        dataNodeT.setNodeId(1);
        dataNodeT.setAdvertiseAddr("localhost:8080");

        replicaNodeT = new ReplicaNodeT();
        replicaNodeT.setDataNode(dataNodeT);
        replicaNodeT.setIsPrimary(true);
    }

    @Test
    void get() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        TreeMap<Long, RangeT> rangeTTreeMap = streamRangeCache.get(0L);
        assertTrue(rangeTTreeMap.isEmpty());
    }

    @Test
    void getError() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(new CacheLoader<Long, TreeMap<Long, RangeT>>() {
            @Override
            public TreeMap<Long, RangeT> load(Long key) throws Exception {
                throw new RuntimeException("test");
            }
        });
        assertThrows(RuntimeException.class, () -> streamRangeCache.get(0L));
    }

    @Test
    void put() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 2L;
        RangeT[] rangeTArray = new RangeT[2];
        rangeTArray[0] = new RangeT();
        rangeTArray[0].setStreamId(streamId);
        rangeTArray[0].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[0].setRangeIndex(0);
        rangeTArray[0].setStartOffset(0);
        rangeTArray[0].setEndOffset(100);
        rangeTArray[0].setNextOffset(100);
        rangeTArray[1] = new RangeT();
        rangeTArray[1].setStreamId(streamId);
        rangeTArray[1].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[1].setRangeIndex(1);
        rangeTArray[1].setStartOffset(100);
        rangeTArray[1].setEndOffset(-1);
        rangeTArray[1].setNextOffset(-1);
        streamRangeCache.put(streamId, rangeTArray);
        AbstractMap<Long, RangeT> treeMap = streamRangeCache.get(streamId);
        assertEquals(rangeTArray.length, treeMap.size());
    }

    @Test
    void refresh() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 3L;
        RangeT rangeT = new RangeT();
        rangeT.setStreamId(streamId);
        rangeT.setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeT.setRangeIndex(0);
        rangeT.setStartOffset(0);
        rangeT.setEndOffset(-1);
        rangeT.setNextOffset(14);
        streamRangeCache.put(streamId, new RangeT[] {rangeT});
        assertEquals(1, streamRangeCache.get(streamId).size());

        streamRangeCache.refresh(streamId);
        assertEquals(0, streamRangeCache.get(streamId).size());

    }

    @Test
    void invalidate() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 4L;
        RangeT rangeT = new RangeT();
        rangeT.setStreamId(streamId);
        rangeT.setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeT.setRangeIndex(0);
        rangeT.setStartOffset(0);
        rangeT.setEndOffset(-1);
        rangeT.setNextOffset(14);
        streamRangeCache.put(streamId, new RangeT[] {rangeT});
        assertEquals(1, streamRangeCache.getSize());

        streamRangeCache.invalidate(streamId);
        assertEquals(0, streamRangeCache.getSize());
    }

    @Test
    void getSize() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        streamRangeCache.get(0L);
        streamRangeCache.get(1L);
        assertEquals(2, streamRangeCache.getSize());
    }

    @Test
    void getFloorRange() throws ExecutionException, InterruptedException {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 6L;
        RangeT[] rangeTArray = new RangeT[3];
        rangeTArray[0] = new RangeT();
        rangeTArray[0].setStreamId(streamId);
        rangeTArray[0].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[0].setRangeIndex(0);
        rangeTArray[0].setStartOffset(0);
        rangeTArray[0].setEndOffset(100);
        rangeTArray[0].setNextOffset(100);
        rangeTArray[1] = new RangeT();
        rangeTArray[1].setStreamId(streamId);
        rangeTArray[1].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[1].setRangeIndex(1);
        rangeTArray[1].setStartOffset(120);
        rangeTArray[1].setEndOffset(150);
        rangeTArray[1].setNextOffset(150);
        rangeTArray[2] = new RangeT();
        rangeTArray[2].setStreamId(streamId);
        rangeTArray[2].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[2].setRangeIndex(2);
        rangeTArray[2].setStartOffset(150);
        rangeTArray[2].setEndOffset(-1);
        rangeTArray[2].setNextOffset(171);
        streamRangeCache.put(streamId, rangeTArray);

        RangeT rangeT = streamRangeCache.getFloorRange(streamId, 11).get();
        assertEquals(0, rangeT.getRangeIndex());
        rangeT = streamRangeCache.getFloorRange(streamId, 100).get();
        assertEquals(1, rangeT.getRangeIndex());
        rangeT = streamRangeCache.getFloorRange(streamId, 190).get();
        assertEquals(2, rangeT.getRangeIndex());
    }

    @Test
    void getLastRange() throws ExecutionException, InterruptedException {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 7L;
        RangeT[] rangeTArray = new RangeT[2];
        rangeTArray[0] = new RangeT();
        rangeTArray[0].setStreamId(streamId);
        rangeTArray[0].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[0].setRangeIndex(0);
        rangeTArray[0].setStartOffset(0);
        rangeTArray[0].setEndOffset(100);
        rangeTArray[0].setNextOffset(100);
        rangeTArray[1] = new RangeT();
        rangeTArray[1].setStreamId(streamId);
        rangeTArray[1].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[1].setRangeIndex(1);
        rangeTArray[1].setStartOffset(120);
        rangeTArray[1].setEndOffset(-1);
        rangeTArray[1].setNextOffset(150);
        streamRangeCache.put(streamId, rangeTArray);

        RangeT rangeT = streamRangeCache.getLastRange(streamId).get();
        assertEquals(1, rangeT.getRangeIndex());
    }

    @Test
    void getLastRangeError() {
        StreamRangeCache streamRangeCache = new StreamRangeCache(emptyCacheLoader);
        long streamId = 8L;
        RangeT[] rangeTArray = new RangeT[2];
        rangeTArray[0] = new RangeT();
        rangeTArray[0].setStreamId(streamId);
        rangeTArray[0].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[0].setRangeIndex(0);
        rangeTArray[0].setStartOffset(0);
        rangeTArray[0].setEndOffset(100);
        rangeTArray[0].setNextOffset(100);
        rangeTArray[1] = new RangeT();
        rangeTArray[1].setStreamId(streamId);
        rangeTArray[1].setReplicaNodes(new ReplicaNodeT[] {replicaNodeT});
        rangeTArray[1].setRangeIndex(1);
        rangeTArray[1].setStartOffset(120);
        rangeTArray[1].setEndOffset(150);
        rangeTArray[1].setNextOffset(150);
        streamRangeCache.put(streamId, rangeTArray);

        assertThrows(ExecutionException.class, () -> streamRangeCache.getLastRange(streamId).get());
    }
}