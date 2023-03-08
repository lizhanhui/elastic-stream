package models;

import java.util.HashMap;
import java.util.Map;

public class Headers {
    private Map<HeaderKey, String> headers;

    public Headers() {
        this.headers = new HashMap<>();
    }

    public void addHeader(HeaderKey key, String value) {
        this.headers.put(key, value);
    }

    public String getHeader(HeaderKey key) {
        return this.headers.get(key);
    }
}

