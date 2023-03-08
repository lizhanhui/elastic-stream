package models;

import java.util.List;

/**
 * A minimum storage unit of stream, each stream is divided into multiple ranges, and only the last range can be written.
 */
public class Range {
    private long streamId;
    private int rangeIndex;
    private long startOffset;
    private long endOffset;
    private long nextOffset;
    private List<ReplicaNode> replicaNodes;
}

class ReplicaNode {
    private DataNode dataNode;
    private boolean isPrimary;
}
