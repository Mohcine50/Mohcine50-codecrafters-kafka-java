package kafka.describeTopicPartitions;

public class PartitionRecordValue extends KafkaRecordValue {
    private final byte[] partitionId;
    private final byte[] topicUUID;
    private final int replicationLength;
    private final int inSyncReplicaArrayLength;
    private final int removingReplicasArrayLength;
    private final CompactArray replicaArray;
    private final CompactArray inSyncReplicaArray;
    private final CompactArray removingReplicasArray;
    private final CompactArray addingReplicaArray;
    private final int addingReplicasCount;
    private final byte[] leader;
    private final byte[] leaderEpoch;
    private final byte[] partitionEpoch;
    private final CompactArray directoriesArray;
    private int directoriesArrayLength;

    private PartitionRecordValue(Builder builder) {
        super(builder);
        this.partitionId = builder.partitionId;
        this.topicUUID = builder.topicUUID;
        this.replicaArray = builder.replicaArray;
        this.inSyncReplicaArray = builder.inSyncReplicaArray;
        this.removingReplicasArray = builder.removingReplicasArray;
        this.addingReplicaArray = builder.addingReplicaArray;
        this.addingReplicasCount = builder.addingReplicasCount;
        this.leader = builder.leader;
        this.leaderEpoch = builder.leaderEpoch;
        this.partitionEpoch = builder.partitionEpoch;
        this.directoriesArray = builder.directoriesArray;
        this.replicationLength = builder.replicaArrayLength;
        this.inSyncReplicaArrayLength = builder.inSyncReplicaArrayLength;
        this.removingReplicasArrayLength = builder.removingReplicasArrayLength;
        this.directoriesArrayLength = builder.directoriesArrayLength;
    }

    public int getDirectoriesArrayLength() {
        return directoriesArrayLength;
    }

    public void setDirectoriesArrayLength(int directoriesArrayLength) {
        this.directoriesArrayLength = directoriesArrayLength;
    }

    public CompactArray getDirectoriesArray() {
        return directoriesArray;
    }

    public byte[] getPartitionEpoch() {
        return partitionEpoch;
    }

    public byte[] getLeaderEpoch() {
        return leaderEpoch;
    }

    public byte[] getLeader() {
        return leader;
    }

    public int getAddingReplicasCount() {
        return addingReplicasCount;
    }

    public CompactArray getRemovingReplicasArray() {
        return removingReplicasArray;
    }

    public CompactArray getInSyncReplicaArray() {
        return inSyncReplicaArray;
    }

    public CompactArray getReplicaArray() {
        return replicaArray;
    }

    public int getRemovingReplicasArrayLength() {
        return removingReplicasArrayLength;
    }

    public int getInSyncReplicaArrayLength() {
        return inSyncReplicaArrayLength;
    }

    public int getReplicationLength() {
        return replicationLength;
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public byte[] getPartitionId() {
        return partitionId;
    }

    public static class Builder extends KafkaRecordValue.Builder<Builder> {
        public CompactArray addingReplicaArray;
        private byte[] partitionId;
        private byte[] topicUUID;
        private CompactArray replicaArray;
        private CompactArray inSyncReplicaArray;
        private CompactArray removingReplicasArray;
        private int addingReplicasCount;
        private byte[] leader;
        private byte[] leaderEpoch;
        private byte[] partitionEpoch;
        private CompactArray directoriesArray;
        private int replicaArrayLength;
        private int inSyncReplicaArrayLength;
        private int removingReplicasArrayLength;
        private int directoriesArrayLength;

        public Builder addingReplicaArray(CompactArray addingReplicaArray) {
            this.addingReplicaArray = addingReplicaArray;
            return this;
        }

        public Builder directoriesArrayLength(int directoriesArrayLength) {
            this.directoriesArrayLength = directoriesArrayLength;
            return this;
        }

        public Builder removingReplicasArrayLength(int removingReplicasArrayLength) {
            this.removingReplicasArrayLength = removingReplicasArrayLength;
            return this;
        }

        public Builder inSyncReplicaArrayLength(int inSyncReplicaArrayLength) {
            this.inSyncReplicaArrayLength = inSyncReplicaArrayLength;
            return this;
        }

        public Builder replicaArrayLength(int replicaArrayLength) {
            this.replicaArrayLength = replicaArrayLength;
            return this;
        }

        public Builder partitionId(byte[] partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder topicUUID(byte[] topicUUID) {
            this.topicUUID = topicUUID;
            return this;
        }

        public Builder replicaArray(CompactArray replicaArray) {
            this.replicaArray = replicaArray;
            return this;
        }

        public Builder inSyncReplicaArray(CompactArray inSyncReplicaArray) {
            this.inSyncReplicaArray = inSyncReplicaArray;
            return this;
        }

        public Builder removingReplicasCount(CompactArray count) {
            this.removingReplicasArray = count;
            return this;
        }

        public Builder addingReplicasCount(int count) {
            this.addingReplicasCount = count;
            return this;
        }

        public Builder leader(byte[] leader) {
            this.leader = leader;
            return this;
        }

        public Builder leaderEpoch(byte[] epoch) {
            this.leaderEpoch = epoch;
            return this;
        }

        public Builder partitionEpoch(byte[] epoch) {
            this.partitionEpoch = epoch;
            return this;
        }

        public Builder directoriesArray(CompactArray directories) {
            this.directoriesArray = directories;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public PartitionRecordValue build() {
            return new PartitionRecordValue(this);
        }
    }
}