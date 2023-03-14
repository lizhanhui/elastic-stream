package client.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * StreamNameIdCache is a cache for StreamName to StreamId mapping.
 */
public class StreamNameIdCache {
    private static final int DEFAULT_CACHE_SIZE = 200;
    private final Cache<String, Long> cache;

    public StreamNameIdCache(int size) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(size)
            // TODO: Synchronous Loading
            .build();
    }

    public StreamNameIdCache() {
        this(DEFAULT_CACHE_SIZE);
    }
}
