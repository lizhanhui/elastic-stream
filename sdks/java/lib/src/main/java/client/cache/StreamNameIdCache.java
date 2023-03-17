package client.cache;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

/**
 * StreamNameIdCache is a cache for StreamName to StreamId mapping.
 */
public class StreamNameIdCache {
    private static final int DEFAULT_CACHE_SIZE = 200;
    private final LoadingCache<String, Long> cache;

    public StreamNameIdCache(CacheLoader<String, Long> loader) {
        this(DEFAULT_CACHE_SIZE, loader);
    }

    public StreamNameIdCache(int size, CacheLoader<String, Long> loader) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(size)
            .build(loader);
    }

    public Long get(String streamName) {
        return cache.get(streamName);
    }

    public void put(String key, Long value) {
        if (key != null && value != null) {
            cache.put(key, value);
        }
    }

    public long getSize() {
        return cache.estimatedSize();
    }
}
