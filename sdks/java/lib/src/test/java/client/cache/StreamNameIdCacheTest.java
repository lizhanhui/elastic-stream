package client.cache;

import com.google.common.cache.CacheLoader;
import org.junit.jupiter.api.Test;
import sdk.elastic.storage.client.cache.StreamNameIdCache;

import static org.junit.jupiter.api.Assertions.*;

class StreamNameIdCacheTest {

    @Test
    void getIfPresent() {

        StreamNameIdCache cache = new StreamNameIdCache(
            new CacheLoader<String, Long>() {
                @Override
                public Long load(String key) {
                    if (key.equals("test")) {
                        return 1L;
                    }
                    return 0L;
                }
            });
        assertEquals(1L, cache.get("test"));
        System.out.println(cache.getSize());
        assertEquals(0L, cache.get("test2"));
        System.out.println(cache.getSize());
    }
}