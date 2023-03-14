package models;

public class Stream {
    private long streamId;
    private int replicaNum;
    private long retentionPeriodMillis;

    public Stream(long streamId, int replicaNums, long retentionPeriodMillis) {
        this.streamId = streamId;
        this.replicaNum = replicaNums;
        this.retentionPeriodMillis = retentionPeriodMillis;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public long getRetentionPeriodMillis() {
        return retentionPeriodMillis;
    }

    public void setRetentionPeriodMillis(long retentionPeriodMillis) {
        this.retentionPeriodMillis = retentionPeriodMillis;
    }
}
