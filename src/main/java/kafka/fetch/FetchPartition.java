package kafka.fetch;

public class FetchPartition {

    private final byte[] partitionId;
    private final byte[] fetchOffset;
    private final byte[] maxBytes;

    private FetchPartition(Builder builder) {
        this.partitionId = builder.partitionId;
        this.fetchOffset = builder.fetchOffset;
        this.maxBytes = builder.maxBytes;
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
        private byte[] fetchOffset;
        private byte[] maxBytes;

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
