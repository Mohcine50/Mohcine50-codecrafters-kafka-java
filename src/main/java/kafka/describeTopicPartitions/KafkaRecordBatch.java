package kafka.describeTopicPartitions;

import java.util.ArrayList;

public class KafkaRecordBatch {

    private byte[] baseOffset;
    private byte[] batchLength;
    private byte[] partitionLeaderEpoch;
    private byte[] magicByte;
    private byte[] crc;
    private byte[] attributes;
    private byte[] lastOffsetDelta;
    private byte[] baseTimestamp;
    private byte[] maxTimestamp;
    private byte[] producerId;
    private byte[] producerEpoch;
    private byte[] baseSequence;
    private byte[] recordsLength;
    private ArrayList<KafkaRecord> records;


    public byte[] getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(byte[] baseOffset) {
        this.baseOffset = baseOffset;
    }

    public byte[] getBatchLength() {
        return batchLength;
    }

    public void setBatchLength(byte[] batchLength) {
        this.batchLength = batchLength;
    }

    public byte[] getPartitionLeaderEpoch() {
        return partitionLeaderEpoch;
    }

    public void setPartitionLeaderEpoch(byte[] partitionLeaderEpoch) {
        this.partitionLeaderEpoch = partitionLeaderEpoch;
    }

    public byte[] getMagicByte() {
        return magicByte;
    }

    public void setMagicByte(byte[] magicByte) {
        this.magicByte = magicByte;
    }

    public byte[] getCrc() {
        return crc;
    }

    public void setCrc(byte[] crc) {
        this.crc = crc;
    }

    public byte[] getAttributes() {
        return attributes;
    }

    public void setAttributes(byte[] attributes) {
        this.attributes = attributes;
    }

    public byte[] getLastOffsetDelta() {
        return lastOffsetDelta;
    }

    public void setLastOffsetDelta(byte[] lastOffsetDelta) {
        this.lastOffsetDelta = lastOffsetDelta;
    }

    public byte[] getBaseTimestamp() {
        return baseTimestamp;
    }

    public void setBaseTimestamp(byte[] baseTimestamp) {
        this.baseTimestamp = baseTimestamp;
    }

    public byte[] getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(byte[] maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public byte[] getProducerId() {
        return producerId;
    }

    public void setProducerId(byte[] producerId) {
        this.producerId = producerId;
    }

    public byte[] getProducerEpoch() {
        return producerEpoch;
    }

    public void setProducerEpoch(byte[] producerEpoch) {
        this.producerEpoch = producerEpoch;
    }

    public byte[] getBaseSequence() {
        return baseSequence;
    }

    public void setBaseSequence(byte[] baseSequence) {
        this.baseSequence = baseSequence;
    }

    public byte[] getRecordsLength() {
        return recordsLength;
    }

    public void setRecordsLength(byte[] recordsLength) {
        this.recordsLength = recordsLength;
    }

    public ArrayList<KafkaRecord> getRecords() {
        return records;
    }

    public void setRecords(ArrayList<KafkaRecord> records) {
        this.records = records;
    }


    public static class Builder {
        private byte[] baseOffset;
        private byte[] batchLength;
        private byte[] partitionLeaderEpoch;
        private byte[] magicByte;
        private byte[] crc;
        private byte[] attributes;
        private byte[] lastOffsetDelta;
        private byte[] baseTimestamp;
        private byte[] maxTimestamp;
        private byte[] producerId;
        private byte[] producerEpoch;
        private byte[] baseSequence;
        private byte[] recordsLength;
        private ArrayList<KafkaRecord> records;


        public Builder setBaseOffset(byte[] baseOffset) {
            this.baseOffset = baseOffset;
            return this;
        }

        public Builder setBatchLength(byte[] batchLength) {
            this.batchLength = batchLength;
            return this;
        }

        public Builder setPartitionLeaderEpoch(byte[] partitionLeaderEpoch) {
            this.partitionLeaderEpoch = partitionLeaderEpoch;
            return this;
        }

        public Builder setMagicByte(byte[] magicByte) {
            this.magicByte = magicByte;
            return this;
        }

        public Builder setCrc(byte[] crc) {
            this.crc = crc;
            return this;
        }

        public Builder setAttributes(byte[] attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder setLastOffsetDelta(byte[] lastOffsetDelta) {
            this.lastOffsetDelta = lastOffsetDelta;
            return this;
        }

        public Builder setBaseTimestamp(byte[] baseTimestamp) {
            this.baseTimestamp = baseTimestamp;
            return this;
        }

        public Builder setMaxTimestamp(byte[] maxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            return this;
        }

        public Builder setProducerId(byte[] producerId) {
            this.producerId = producerId;
            return this;
        }

        public Builder setProducerEpoch(byte[] producerEpoch) {
            this.producerEpoch = producerEpoch;
            return this;
        }

        public Builder setBaseSequence(byte[] baseSequence) {
            this.baseSequence = baseSequence;
            return this;
        }

        public Builder setRecordsLength(byte[] recordsLength) {
            this.recordsLength = recordsLength;
            return this;
        }

        public Builder setRecords(ArrayList<KafkaRecord> records) {
            this.records = records;
            return this;
        }

        public KafkaRecordBatch build() {
            KafkaRecordBatch kafkaRecordBatch = new KafkaRecordBatch();
            kafkaRecordBatch.setBaseOffset(this.baseOffset);
            kafkaRecordBatch.setBatchLength(this.batchLength);
            kafkaRecordBatch.setPartitionLeaderEpoch(this.partitionLeaderEpoch);
            kafkaRecordBatch.setMagicByte(this.magicByte);
            kafkaRecordBatch.setCrc(this.crc);
            kafkaRecordBatch.setAttributes(this.attributes);
            kafkaRecordBatch.setLastOffsetDelta(this.lastOffsetDelta);
            kafkaRecordBatch.setBaseTimestamp(this.baseTimestamp);
            kafkaRecordBatch.setMaxTimestamp(this.maxTimestamp);
            kafkaRecordBatch.setProducerId(this.producerId);
            kafkaRecordBatch.setProducerEpoch(this.producerEpoch);
            kafkaRecordBatch.setBaseSequence(this.baseSequence);
            kafkaRecordBatch.setRecordsLength(this.recordsLength);
            kafkaRecordBatch.setRecords(this.records);
            return kafkaRecordBatch;
        }
    }

}
