package client.common;

import java.util.concurrent.atomic.AtomicInteger;

public class RequestId {
    private static final AtomicInteger INDEX = new AtomicInteger(0);
    private final int id;

    public RequestId() {
        this.id = INDEX.getAndIncrement();
    }

    /**
     * Request ID within current process.
     *
     * @return index.
     */
    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
