package kafka.describeTopicPartitions;

public class FeatureLevelRecord extends KafkaRecordValue {
    private final int nameLength;
    private final byte[] name;
    private final byte[] featureLevel;

    private FeatureLevelRecord(Builder builder) {
        super(builder);
        this.nameLength = builder.nameLength;
        this.name = builder.name;
        this.featureLevel = builder.featureLevel;
    }

    public byte[] getFeatureLevel() {
        return featureLevel;
    }

    public byte[] getName() {
        return name;
    }

    public int getNameLength() {
        return nameLength;
    }

    public static class Builder extends KafkaRecordValue.Builder<Builder> {
        private int nameLength;
        private byte[] name;
        private byte[] featureLevel;

        public Builder nameLength(int nameLength) {
            this.nameLength = nameLength;
            return this;
        }

        public Builder name(byte[] name) {
            this.name = name;
            return this;
        }

        public Builder featureLevel(byte[] featureLevel) {
            this.featureLevel = featureLevel;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public FeatureLevelRecord build() {
            return new FeatureLevelRecord(this);
        }
    }
}
