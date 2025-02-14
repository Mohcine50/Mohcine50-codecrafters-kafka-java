package kafka.describeTopicPartitions;

public class KafkaRecordValue {

    private byte[] frameVersion;
    private byte[] type;
    private byte[] version;
    private byte[] nameLength;
    private byte[] topicName;
    private byte[] topicUUID;
    private byte[] taggedFieldsCount;

    public byte[] getTaggedFieldsCount() {
        return taggedFieldsCount;
    }

    public void setTaggedFieldsCount(byte[] taggedFieldsCount) {
        this.taggedFieldsCount = taggedFieldsCount;
    }

    public byte[] getTopicUUID() {
        return topicUUID;
    }

    public void setTopicUUID(byte[] topicUUID) {
        this.topicUUID = topicUUID;
    }

    public byte[] getTopicName() {
        return topicName;
    }

    public void setTopicName(byte[] topicName) {
        this.topicName = topicName;
    }

    public byte[] getNameLength() {
        return nameLength;
    }

    public void setNameLength(byte[] nameLength) {
        this.nameLength = nameLength;
    }

    public byte[] getVersion() {
        return version;
    }

    public void setVersion(byte[] version) {
        this.version = version;
    }

    public byte[] getType() {
        return type;
    }

    public void setType(byte[] type) {
        this.type = type;
    }

    public byte[] getFrameVersion() {
        return frameVersion;
    }

    public void setFrameVersion(byte[] frameVersion) {
        this.frameVersion = frameVersion;
    }


    public static class Builder {
        private byte[] frameVersion;
        private byte[] type;
        private byte[] version;
        private byte[] nameLength;
        private byte[] topicName;
        private byte[] topicUUID;
        private byte[] taggedFieldsCount;

        public Builder setFrameVersion(byte[] frameVersion) {
            this.frameVersion = frameVersion;
            return this;
        }

        public Builder setType(byte[] type) {
            this.type = type;
            return this;
        }

        public Builder setVersion(byte[] version) {
            this.version = version;
            return this;
        }

        public Builder setNameLength(byte[] nameLength) {
            this.nameLength = nameLength;
            return this;
        }

        public Builder setTopicName(byte[] topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder setTopicUUID(byte[] topicUUID) {
            this.topicUUID = topicUUID;
            return this;
        }

        public Builder setTaggedFieldsCount(byte[] taggedFieldsCount) {
            this.taggedFieldsCount = taggedFieldsCount;
            return this;
        }

        public KafkaRecordValue build() {
            KafkaRecordValue kafkaRecordValue = new KafkaRecordValue();
            kafkaRecordValue.setFrameVersion(this.frameVersion);
            kafkaRecordValue.setType(this.type);
            kafkaRecordValue.setVersion(this.version);
            kafkaRecordValue.setNameLength(this.nameLength);
            kafkaRecordValue.setTopicName(this.topicName);
            kafkaRecordValue.setTopicUUID(this.topicUUID);
            kafkaRecordValue.setTaggedFieldsCount(this.taggedFieldsCount);
            return kafkaRecordValue;
        }
    }

}
