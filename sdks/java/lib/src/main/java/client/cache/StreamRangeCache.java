package client.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import models.StreamRange;

/**
 * StreamRangeCache is a cache for StreamId to StreamRanges mapping.
 */
public class StreamRangeCache {
    private static final int DEFAULT_CACHE_SIZE = 100;

    private final Cache<Long, List<StreamRange>> cache;

    public StreamRangeCache(int size) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(size)
            // TODO: Synchronous Loading
            .build();
    }

    public StreamRangeCache() {
        this(DEFAULT_CACHE_SIZE);
    }
}
