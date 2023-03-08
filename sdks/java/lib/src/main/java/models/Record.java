package models;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Record {
    private long streamId;
    private Headers headers;
    private Map<String, String> properties;
    private ByteBuffer body;
}
