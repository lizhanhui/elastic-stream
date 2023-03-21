package client.cache;

import apis.exception.ClientException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import header.RangeT;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * StreamRangeCache is a cache for StreamId to StreamRanges mapping.
 */
public class StreamRangeCache {
    private static final int DEFAULT_CACHE_SIZE = 100;

    private final LoadingCache<Long, TreeMap<Long, RangeT>> cache;

    /**
     * Create a new StreamRangeCache with the specified size and CacheLoader.
     *
     * @param size   - the maximum number of entries the cache may contain
     * @param loader - the CacheLoader used to compute values
     */
    public StreamRangeCache(int size, CacheLoader<Long, TreeMap<Long, RangeT>> loader) {
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .build(loader);
    }

    /**
     * Create a new StreamRangeCache with the specified CacheLoader.
     * The cache size is set to the default value.
     *
     * @param loader - the CacheLoader used to compute values
     */
    public StreamRangeCache(CacheLoader<Long, TreeMap<Long, RangeT>> loader) {
        this(DEFAULT_CACHE_SIZE, loader);
    }

    /**
     * Get stream ranges by stream id.
     * Returns the value associated with the key in this cache, obtaining that value from CacheLoader.load(Object) if necessary.
     *
     * @param streamId with which the specified ranges is to be associated
     * @return stream ranges, the current (existing or computed) ranges associated with the specified streamId, or null if the computed value is null
     */
    public TreeMap<Long, RangeT> get(Long streamId) {
        return cache.getUnchecked(streamId);
    }

    /**
     * Put stream ranges by stream id.
     * If streamId is null or ranges is null or ranges is empty, do nothing.
     *
     * @param streamId - with which the specified ranges is to be associated
     * @param ranges   - the ranges to be associated with the specified streamId
     */
    public void put(Long streamId, RangeT[] ranges) {
        if (streamId != null && ranges != null && ranges.length > 0) {
            TreeMap<Long, RangeT> rangesMap = new TreeMap<>();
            for (RangeT range : ranges) {
                rangesMap.put(range.getStartOffset(), range);
            }
            cache.put(streamId, rangesMap);
        }
    }

    /**
     * Refresh stream ranges by stream id.
     *
     * @param streamId - with which the specified ranges is to be associated
     */
    public void refresh(Long streamId) {
        cache.refresh(streamId);
    }

    /**
     * Invalidate stream ranges by stream id.
     *
     * @param streamId - with which the specified ranges is to be associated
     */
    public void invalidate(Long streamId) {
        cache.invalidate(streamId);
    }

    /**
     * Returns the approximate number of entries in this cache. The value returned is an estimate.
     *
     * @return the estimated size of this cache.
     */
    public long getSize() {
        return cache.size();
    }

    /**
     * Get the floor range, which contains valid recordBatch based on the given startOffset.
     * The returned range maybe:
     * <p>a writable range (endOffset < 0), or</p>
     * <p>a sealed range where startOffset is in (startOffset <= offset < endOffset), or</p>
     * <p>the higher range (with the least startOffset > offset).</p>
     * This method is generally used to get the range for fetching.
     *
     * @param streamId    with which the specified ranges is to be associated
     * @param startOffset the offset to search
     * @return the floor range
     */
    public CompletableFuture<RangeT> getFloorRange(long streamId, long startOffset) {
        assert streamId > 0 && startOffset >= 0;
        CompletableFuture<RangeT> completableFuture = new CompletableFuture<>();

        try {
            TreeMap<Long, RangeT> rangesMap = get(streamId);
            AbstractMap.Entry<Long, RangeT> floorEntry = rangesMap.floorEntry(startOffset);

            if (floorEntry == null || floorEntry.getValue() == null) {
                completableFuture.completeExceptionally(new ClientException("Floor range is null for stream " + streamId + " with offset " + startOffset));
                return completableFuture;
            }

            // If the floor range is sealed and the offset is in compaction range, we need to find the next range.
            if (floorEntry.getValue().getEndOffset() > 0 && floorEntry.getValue().getEndOffset() <= startOffset) {
                Map.Entry<Long, RangeT> entry = rangesMap.higherEntry(startOffset);
                if (entry == null || entry.getValue() == null) {
                    completableFuture.completeExceptionally(new ClientException("Floor range is null for stream " + streamId + " with offset " + startOffset));
                    return completableFuture;
                }
                completableFuture.complete(entry.getValue());
            } else {
                completableFuture.complete(floorEntry.getValue());
            }
        } catch (Throwable ex) {
            completableFuture.completeExceptionally(ex);
        }

        return completableFuture;
    }

    /**
     * Get the last range of the stream.
     *
     * @param streamId with which the specified ranges is to be associated
     * @return the last range
     */
    public CompletableFuture<RangeT> getLastRange(long streamId) {
        assert streamId > 0;
        CompletableFuture<RangeT> completableFuture = new CompletableFuture<>();

        try {
            RangeT targetRange = get(streamId).lastEntry().getValue();
            // Data nodes may have been ready. Try again.
            if (targetRange.getReplicaNodes().length == 0) {
                refresh(streamId);
                targetRange = get(streamId).lastEntry().getValue();
            }
            if (targetRange.getReplicaNodes().length == 0) {
                completableFuture.completeExceptionally(new ClientException("Failed to get ReplicaNodes of the last range of stream " + streamId));
                return completableFuture;
            }
            completableFuture.complete(targetRange);
        } catch (Throwable ex) {
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }
}
