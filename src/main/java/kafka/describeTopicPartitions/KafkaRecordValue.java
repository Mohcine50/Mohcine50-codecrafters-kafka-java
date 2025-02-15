package kafka.describeTopicPartitions;

public class KafkaRecordValue {
    protected final byte[] frameVersion;
    protected final byte[] type;
    protected final byte[] version;
    protected final int taggedFieldsCount;

    protected KafkaRecordValue(Builder<?> builder) {
        this.frameVersion = builder.frameVersion;
        this.type = builder.type;
        this.version = builder.version;
        this.taggedFieldsCount = builder.taggedFieldsCount;
    }

    public abstract static class Builder<T extends Builder<T>> {
        private byte[] frameVersion;
        private byte[] type;
        private byte[] version;
        private int taggedFieldsCount;

        public T frameVersion(byte[] frameVersion) {
            this.frameVersion = frameVersion;
            return self();
        }

        public T type(byte[] type) {
            this.type = type;
            return self();
        }

        public T version(byte[] version) {
            this.version = version;
            return self();
        }

        public T taggedFieldsCount(int taggedFieldsCount) {
            this.taggedFieldsCount = taggedFieldsCount;
            return self();
        }

        protected abstract T self();

        public abstract KafkaRecordValue build();
    }
}

