package kafka.fetch;

public class FetchPartition {

    private final byte[] partitionId;
    private final byte[] currentLeaderEpoch;
    private final byte[] lastFetchEpoch;
    private final byte[] logStartOffset;
    private final byte[] fetchOffset;
    private final byte[] maxBytes;

    private FetchPartition(Builder builder) {
        this.partitionId = builder.partitionId;
        this.fetchOffset = builder.fetchOffset;
        this.maxBytes = builder.maxBytes;
        this.currentLeaderEpoch = builder.currentLeaderEpoch;
        this.lastFetchEpoch = builder.lastFetchEpoch;
        this.logStartOffset = builder.logStartOffset;
    }

    public byte[] getCurrentLeaderEpoch() {
        return currentLeaderEpoch;
    }

    public byte[] getLastFetchEpoch() {
        return lastFetchEpoch;
    }

    public byte[] getLogStartOffset() {
        return logStartOffset;
    }

    public byte[] getPartitionId() {
        return partitionId;
    }

    public byte[] getFetchOffset() {
        return fetchOffset;
    }

    public byte[] getMaxBytes() {
        return maxBytes;
    }

    public static class Builder {
        private byte[] partitionId;
        private byte[] currentLeaderEpoch;
        private byte[] lastFetchEpoch;
        private byte[] logStartOffset;
        private byte[] fetchOffset;
        private byte[] maxBytes;


        public Builder currentLeaderEpoch(byte[] currentLeaderEpoch) {
            this.currentLeaderEpoch = currentLeaderEpoch;
            return this;
        }

        public Builder lastFetchEpoch(byte[] lastFetchEpoch) {
            this.lastFetchEpoch = lastFetchEpoch;
            return this;
        }

        public Builder logStartOffset(byte[] logStartOffset) {
            this.logStartOffset = logStartOffset;
            return this;
        }


        public Builder partitionId(byte[] partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder fetchOffset(byte[] fetchOffset) {
            this.fetchOffset = fetchOffset;
            return this;
        }

        public Builder maxBytes(byte[] maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public FetchPartition build() {
            return new FetchPartition(this);
        }
    }
}
