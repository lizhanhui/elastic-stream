package com.automq.elasticstream.client.tools.e2e;

public class E2EOption {
    private String endpoint = "127.0.0.1:12378";
    private String kvEndpoint = "127.0.0.1:12379";
    private long count = 100;
    private long streamId = -1;
    private long startSeq = 0;
    private int replica = 1;
    private int batchSize = 10;

    public E2EOption() {
        String endpoint = System.getenv("E2E_END_POINT");
        if (endpoint != null) {
            this.endpoint = endpoint;
        }
        String kvEndpoint = System.getenv("E2E_KV_END_POINT");
        if (kvEndpoint != null) {
            this.kvEndpoint = kvEndpoint;
        }
        String count = System.getenv("E2E_COUNT");
        if (count != null) {
            this.count = Long.parseLong(count);
        }
        String streamId = System.getenv("E2E_STREAM_ID");
        if (streamId != null) {
            this.streamId = Long.parseLong(streamId);
        }
        String startSeq = System.getenv("E2E_START_SEQ");
        if (startSeq != null) {
            this.startSeq = Long.parseLong(startSeq);
        }
        String replica = System.getenv("E2E_REPLICA");
        if (replica != null) {
            this.replica = Integer.parseInt(replica);
        }
        String batchSize = System.getenv("E2E_BATCH_SIZE");
        if (batchSize != null) {
            this.batchSize = Integer.parseInt(batchSize);
        }
    }

    public String getEndPoint() {
        return this.endpoint;
    }

    public String getKvEndPoint() {
        return this.kvEndpoint;
    }

    public long getCount() {
        return this.count;
    }

    public long getStreamId() {
        return this.streamId;
    }

    public long getStartSeq() {
        return this.startSeq;
    }

    public int getReplica() {
        return this.replica;
    }

    public int getBatchSize() {
        return this.batchSize;
    }
}
