package kafka.describeTopicPartitions;

public class TopicRecordValue extends KafkaRecordValue {
    private final int nameLength;
    private final byte[] topicName;
    private final byte[] topicUUID;

    private TopicRecordValue(Builder builder) {
        super(builder);
        this.nameLength = builder.nameLength;
        this.topicName = builder.topicName;
        this.topicUUID = builder.topicUUID;
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public byte[] getTopicName() {
        return topicName;
    }

    public int getNameLength() {
        return nameLength;
    }

    public static class Builder extends KafkaRecordValue.Builder<Builder> {
        private int nameLength;
        private byte[] topicName;
        private byte[] topicUUID;

        public Builder nameLength(int nameLength) {
            this.nameLength = nameLength;
            return this;
        }

        public Builder topicName(byte[] topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder topicUUID(byte[] topicUUID) {
            this.topicUUID = topicUUID;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public TopicRecordValue build() {
            return new TopicRecordValue(this);
        }
    }
}
