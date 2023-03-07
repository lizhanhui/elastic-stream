package models;

public class Stream {
    private long streamId;
    private int replicaNums;
    private long retentionPeriodMS;

    public Stream(long streamId, int replicaNums, long retentionPeriodMS) {
        this.streamId = streamId;
        this.replicaNums = replicaNums;
        this.retentionPeriodMS = retentionPeriodMS;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public int getReplicaNums() {
        return replicaNums;
    }

    public void setReplicaNums(int replicaNums) {
        this.replicaNums = replicaNums;
    }

    public long getRetentionPeriodMS() {
        return retentionPeriodMS;
    }

    public void setRetentionPeriodMS(long retentionPeriodMS) {
        this.retentionPeriodMS = retentionPeriodMS;
    }
}
