package client.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StreamNameIdCacheTest {

    @Test
    void getIfPresent() {
        StreamNameIdCache cache = new StreamNameIdCache(key -> {
            if (key.equals("test")) {
                return 1L;
            }
            return null;
        });
        assertEquals(1L, cache.get("test"));
        System.out.println(cache.getSize());
        assertNull(cache.get("test2"));
        System.out.println(cache.getSize());
    }
}