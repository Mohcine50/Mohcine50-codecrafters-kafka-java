package kafka.describeTopicPartitions;

public class PartitionRecordValue extends KafkaRecordValue {
    private final byte[] partitionId;
    private final byte[] topicUUID;
    private final int replicationLength;
    private final int inSyncReplicaArrayLength;
    private final int removingReplicasArrayLength;
    private final byte[] replicaArray;
    private final byte[] inSyncReplicaArray;
    private final byte[] removingReplicasArray;
    private final int addingReplicasCount;
    private final byte[] leader;
    private final byte[] leaderEpoch;
    private final byte[] partitionEpoch;
    private final byte[] directoriesArray;
    private int directoriesArrayLength;

    private PartitionRecordValue(Builder builder) {
        super(builder);
        this.partitionId = builder.partitionId;
        this.topicUUID = builder.topicUUID;
        this.replicaArray = builder.replicaArray;
        this.inSyncReplicaArray = builder.inSyncReplicaArray;
        this.removingReplicasArray = builder.removingReplicasArray;
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

    public byte[] getDirectoriesArray() {
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

    public byte[] getRemovingReplicasArray() {
        return removingReplicasArray;
    }

    public byte[] getInSyncReplicaArray() {
        return inSyncReplicaArray;
    }

    public byte[] getReplicaArray() {
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
        private byte[] partitionId;
        private byte[] topicUUID;
        private byte[] replicaArray;
        private byte[] inSyncReplicaArray;
        private byte[] removingReplicasArray;
        private int addingReplicasCount;
        private byte[] leader;
        private byte[] leaderEpoch;
        private byte[] partitionEpoch;
        private byte[] directoriesArray;
        private int replicaArrayLength;
        private int inSyncReplicaArrayLength;
        private int removingReplicasArrayLength;
        private int directoriesArrayLength;

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

        public Builder replicaArray(byte[] replicaArray) {
            this.replicaArray = replicaArray;
            return this;
        }

        public Builder inSyncReplicaArray(byte[] inSyncReplicaArray) {
            this.inSyncReplicaArray = inSyncReplicaArray;
            return this;
        }

        public Builder removingReplicasCount(byte[] count) {
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

        public Builder directoriesArray(byte[] directories) {
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