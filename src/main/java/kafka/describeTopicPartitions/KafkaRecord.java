package kafka.describeTopicPartitions;

public class KafkaRecord {
    private int length;
    private byte[] attributes;
    private int timestampDelta;
    private int offsetDelta;
    private int keyLength;
    private byte[] key;
    private int valueLength;
    private KafkaRecordValue value; // Topic Record
    private int headersArrayCount;

    public int getHeadersArrayCount() {
        return headersArrayCount;
    }

    public void setHeadersArrayCount(int headersArrayCount) {
        this.headersArrayCount = headersArrayCount;
    }

    public KafkaRecordValue getValue() {
        return value;
    }

    public void setValue(KafkaRecordValue value) {
        this.value = value;
    }

    public int getValueLength() {
        return valueLength;
    }

    public void setValueLength(int valueLength) {
        this.valueLength = valueLength;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getKeyLength() {
        return keyLength;
    }

    public void setKeyLength(int keyLength) {
        this.keyLength = keyLength;
    }

    public int getOffsetDelta() {
        return offsetDelta;
    }

    public void setOffsetDelta(int offsetDelta) {
        this.offsetDelta = offsetDelta;
    }

    public int getTimestampDelta() {
        return timestampDelta;
    }

    public void setTimestampDelta(int timestampDelta) {
        this.timestampDelta = timestampDelta;
    }

    public byte[] getAttributes() {
        return attributes;
    }

    public void setAttributes(byte[] attributes) {
        this.attributes = attributes;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public static class Builder {
        private int length;
        private byte[] attributes;
        private int timestampDelta;
        private int offsetDelta;
        private int keyLength;
        private byte[] key;
        private int valueLength;
        private KafkaRecordValue value;
        private int headersArrayCount;

        public Builder setLength(int length) {
            this.length = length;

            return this;
        }

        public Builder setAttributes(byte[] attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder setTimestampDelta(int timestampDelta) {
            this.timestampDelta = timestampDelta;
            return this;
        }

        public Builder setOffsetDelta(int offsetDelta) {
            this.offsetDelta = offsetDelta;
            return this;
        }

        public Builder setKeyLength(int keyLength) {
            this.keyLength = keyLength;
            return this;
        }

        public Builder setKey(byte[] key) {
            this.key = key;
            return this;
        }

        public Builder setValueLength(int valueLength) {
            this.valueLength = valueLength;
            return this;
        }

        public Builder setValue(KafkaRecordValue value) {
            this.value = value;
            return this;
        }

        public Builder setHeadersArrayCount(int headersArrayCount) {
            this.headersArrayCount = headersArrayCount;
            return this;
        }

        public KafkaRecord build() {
            KafkaRecord kafkaRecord = new KafkaRecord();
            kafkaRecord.setLength(this.length);
            kafkaRecord.setAttributes(this.attributes);
            kafkaRecord.setTimestampDelta(this.timestampDelta);
            kafkaRecord.setOffsetDelta(this.offsetDelta);
            kafkaRecord.setKeyLength(this.keyLength);
            kafkaRecord.setKey(this.key);
            kafkaRecord.setValueLength(this.valueLength);
            kafkaRecord.setValue(this.value);
            kafkaRecord.setHeadersArrayCount(this.headersArrayCount);
            return kafkaRecord;
        }
    }

}
