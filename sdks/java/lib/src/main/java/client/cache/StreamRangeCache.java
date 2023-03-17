package client.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import header.Range;

/**
 * StreamRangeCache is a cache for StreamId to StreamRanges mapping.
 */
public class StreamRangeCache {
    private static final int DEFAULT_CACHE_SIZE = 100;

    private final LoadingCache<Long, List<Range>> cache;

    /**
     * Create a new StreamRangeCache with the specified size and CacheLoader.
     * @param size - the maximum number of entries the cache may contain
     * @param loader - the CacheLoader used to compute values
     */
    public StreamRangeCache(int size, CacheLoader<Long, List<Range>> loader) {
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .build(loader);
    }

    /**
     * Create a new StreamRangeCache with the specified CacheLoader.
     * The cache size is set to the default value.
     * @param loader - the CacheLoader used to compute values
     */
    public StreamRangeCache(CacheLoader<Long, List<Range>> loader) {
        this(DEFAULT_CACHE_SIZE, loader);
    }

    /**
     * Get stream ranges by stream id.
     * Returns the value associated with the key in this cache, obtaining that value from CacheLoader.load(Object) if necessary.
     *
     * @param streamId - with which the specified ranges is to be associated
     * @return stream ranges - the current (existing or computed) ranges associated with the specified streamId, or null if the computed value is null
     */
    public List<Range> get(Long streamId) {
        return cache.getUnchecked(streamId);
    }

    /**
     * Put stream ranges by stream id.
     * If streamId is null or ranges is null or ranges is empty, do nothing.
     * @param streamId - with which the specified ranges is to be associated
     * @param ranges - the ranges to be associated with the specified streamId
     */
    public void put(Long streamId, List<Range> ranges) {
        if (streamId != null && ranges != null && ranges.size() > 0) {
            cache.put(streamId, ranges);
        }
    }

    /**
     * Returns the approximate number of entries in this cache. The value returned is an estimate.
     * @return the estimated size of this cache.
     */
    public long getSize() {
        return cache.size();
    }
}
