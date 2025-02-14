package kafka.describeTopicPartitions;

public class KafkaRecord {
    private byte[] length;
    private byte[] attributes;
    private byte[] timestampDelta;
    private byte[] offsetDelta;
    private byte[] keyLength;
    private byte[] key;
    private byte[] valueLength;
    private KafkaRecordValue value; // Topic Record
    private byte[] headersArrayCount;

    public byte[] getHeadersArrayCount() {
        return headersArrayCount;
    }

    public void setHeadersArrayCount(byte[] headersArrayCount) {
        this.headersArrayCount = headersArrayCount;
    }

    public KafkaRecordValue getValue() {
        return value;
    }

    public void setValue(KafkaRecordValue value) {
        this.value = value;
    }

    public byte[] getValueLength() {
        return valueLength;
    }

    public void setValueLength(byte[] valueLength) {
        this.valueLength = valueLength;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKeyLength() {
        return keyLength;
    }

    public void setKeyLength(byte[] keyLength) {
        this.keyLength = keyLength;
    }

    public byte[] getOffsetDelta() {
        return offsetDelta;
    }

    public void setOffsetDelta(byte[] offsetDelta) {
        this.offsetDelta = offsetDelta;
    }

    public byte[] getTimestampDelta() {
        return timestampDelta;
    }

    public void setTimestampDelta(byte[] timestampDelta) {
        this.timestampDelta = timestampDelta;
    }

    public byte[] getAttributes() {
        return attributes;
    }

    public void setAttributes(byte[] attributes) {
        this.attributes = attributes;
    }

    public byte[] getLength() {
        return length;
    }

    public void setLength(byte[] length) {
        this.length = length;
    }

    public static class Builder {
        private byte[] length;
        private byte[] attributes;
        private byte[] timestampDelta;
        private byte[] offsetDelta;
        private byte[] keyLength;
        private byte[] key;
        private byte[] valueLength;
        private KafkaRecordValue value;
        private byte[] headersArrayCount;

        public Builder setLength(byte[] length) {
            this.length = length;
            return this;
        }

        public Builder setAttributes(byte[] attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder setTimestampDelta(byte[] timestampDelta) {
            this.timestampDelta = timestampDelta;
            return this;
        }

        public Builder setOffsetDelta(byte[] offsetDelta) {
            this.offsetDelta = offsetDelta;
            return this;
        }

        public Builder setKeyLength(byte[] keyLength) {
            this.keyLength = keyLength;
            return this;
        }

        public Builder setKey(byte[] key) {
            this.key = key;
            return this;
        }

        public Builder setValueLength(byte[] valueLength) {
            this.valueLength = valueLength;
            return this;
        }

        public Builder setValue(KafkaRecordValue value) {
            this.value = value;
            return this;
        }

        public Builder setHeadersArrayCount(byte[] headersArrayCount) {
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
