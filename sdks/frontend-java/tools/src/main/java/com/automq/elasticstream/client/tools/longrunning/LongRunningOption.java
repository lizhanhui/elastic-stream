package com.automq.elasticstream.client.tools.longrunning;

public class LongRunningOption {
    private String endpoint = "127.0.0.1:12378";
    private String kvEndpoint = "127.0.0.1:12379";
    private long appendInterval = 100;
    private int payloadSizeMin = 1024;
    private int payloadSizeMax = 4096;

    public LongRunningOption() {
        String endpoint = System.getenv("END_POINT");
        if (endpoint != null) {
            this.endpoint = endpoint;
        }
        String kvEndpoint = System.getenv("KV_END_POINT");
        if (kvEndpoint != null) {
            this.kvEndpoint = kvEndpoint;
        }
        String intervalStr = System.getenv("APPEND_INTERVAL");
        if (intervalStr != null) {
            this.appendInterval = Long.parseLong(intervalStr);
        }
        String minStr = System.getenv("PAYLOAD_SIZE_MIN");
        if (minStr != null) {
            this.payloadSizeMin = Integer.parseInt(minStr);
        }
        String maxStr = System.getenv("PAYLOAD_SIZE_MAX");
        if (maxStr != null) {
            this.payloadSizeMax = Integer.parseInt(maxStr);
        }
    }

    public String getEndPoint() {
        return this.endpoint;
    }

    public String getKvEndPoint() {
        return this.kvEndpoint;
    }

    public long getInterval() {
        return this.appendInterval;
    }

    public int getMin() {
        return this.payloadSizeMin;
    }

    public int getMax() {
        return this.payloadSizeMax;
    }
}
