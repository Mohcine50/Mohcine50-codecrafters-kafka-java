package kafka.describeTopicPartitions;

import kafka.apiVersion.ApiVersionRequest;
import kafka.request.KafkaRequest;
import kafka.request.RequestInterface;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static lib.Constants.KAFKA_METADATA_CLUSTER_LOG_FILE_PATH;
import static lib.Utils.readVarint;

/**
 * KAFKA REQUEST CONTENT
 * - Message Size 4 bytes
 * ↓ Request Header (v2)
 * - API Key 2 bytes
 * - API Version 2 bytes
 * - Correlation ID 4 bytes
 * ↓ Client ID
 * - Length 2-byte integer indicating the length of the Client ID string
 * - Contents encoded in UTF-8
 * - Tag buffer
 * → DescribeTopicPartitions Request Body (v0)
 * ↓ Topics Array
 * - Array Length
 * → Topic
 * - Topic Name Length
 * - Topic Name
 * - Topic Tag Buffer
 */

public class DescribeTopicPartitionsRequest extends KafkaRequest implements RequestInterface {

    private Socket clientSocket;

    private byte[] content;
    private byte[] topicName;
    private byte[] topicNameLength;
    private byte[] topicArrayLength;
    private byte[] partitionLimits;
    private byte[] cursor;
    private byte[] partitionTopicName;
    private byte[] partitionIndex;
    private byte[] partitionTopicNameLength;
    private List<KafkaRecordBatch> kafkaRecordBatchs = new ArrayList<KafkaRecordBatch>();

    public DescribeTopicPartitionsRequest() {
        super();
    }


    @Override
    public void handleRequest() {
        // Implementation goes here
    }

    @Override
    public void parseStream(InputStream in) {
        // Implementation goes here
    }

    @Override
    public void sendResponse(OutputStream out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        handleKafkaMetaDataCluster();

        System.out.println("Kafka batches length: " + kafkaRecordBatchs.size());
        System.out.println("Kafka batches records length: " + kafkaRecordBatchs.getFirst().getRecords().size());


        // Correlation Id
        baos.write(this.getCorrelationId());
        // tag buffer
        baos.write(new byte[]{0});

        // Throttle Time
        baos.write(new byte[]{0, 0, 0, 0});

        // Array Length : The length of the topics array + 1
        baos.write(topicArrayLength);

        // Error Code: Error COde for partition
        baos.write(new byte[]{0, 0});
        // Topic Name Length
        baos.write(topicNameLength);
        // Topic Name Content
        baos.write(topicName);
        // Topic ID
        baos.write(new byte[16]);
        // Is Internal
        baos.write(new byte[]{0});
        // Partitions Array
        baos.write(new byte[]{1});
        // Topic Authorized Operations
        baos.write(new byte[]{0, 0, (byte) 0x0d, (byte) 0xf8});
        baos.write(new byte[]{0});
        baos.write(0xFF);
        baos.write(new byte[]{0});

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(baos.toByteArray());
        out.flush();
    }

    InputStream getMetaDataLogFileInputStream() throws FileNotFoundException {

        Path path = Path.of(KAFKA_METADATA_CLUSTER_LOG_FILE_PATH);

        System.out.println("Path: " + path);

        return new FileInputStream(path.toFile());
    }

    public void handleKafkaMetaDataCluster() throws IOException {

        InputStream in = getMetaDataLogFileInputStream();


        while (in.available() > 0) {
            System.out.println("INSIDE");
            KafkaRecordBatch kafkaRecordBatch = new KafkaRecordBatch.Builder()
                    // - Base Offset: 8 bytes
                    .setBaseOffset(in.readNBytes(8))
                    // - Batch Length: 4 bytes
                    .setBatchLength(in.readNBytes(4))
                    // - Partition Leader Epoch: 4 bytes
                    .setPartitionLeaderEpoch(in.readNBytes(4))
                    // - Magic Byte: 1 byte
                    .setMagicByte(in.readNBytes(1))
                    // - CRC: 4 bytes
                    .setCrc(in.readNBytes(4))
                    // - Attributes: 2 bytes
                    .setAttributes(in.readNBytes(2))
                    // - Last Offset Delta: 4 bytes
                    .setLastOffsetDelta(in.readNBytes(4))
                    // - Base Timestamp: 8 bytes
                    .setBaseTimestamp(in.readNBytes(8))
                    // - Max Timestamp: 8 bytes
                    .setMaxTimestamp(in.readNBytes(8))
                    // - Producer ID: 8 bytes
                    .setProducerId(in.readNBytes(8))
                    // - Producer Epoch: 2 bytes
                    .setProducerEpoch(in.readNBytes(2))
                    // - Base Sequence: 4 bytes
                    .setBaseSequence(in.readNBytes(2))
                    .build();
            // - Records Length: Records Length is a 4-byte big-endian integer indicating the number of records in this batch.

            byte[] recordsLength = in.readNBytes(4);

            kafkaRecordBatch.setRecordsLength(recordsLength);
            kafkaRecordBatch.setRecords(getKafkaRecords(in, recordsLength));

            kafkaRecordBatchs.add(kafkaRecordBatch);
            System.out.println(kafkaRecordBatch);
            System.out.println("finish");
        }
        System.out.println("Kafka batches length: " + kafkaRecordBatchs.size());
        System.out.println("Kafka batches records length: " + kafkaRecordBatchs.getFirst().getRecords().size());


    }

    private ArrayList<KafkaRecord> getKafkaRecords(InputStream in, byte[] recordsLength) throws IOException {

        ArrayList<KafkaRecord> records = new ArrayList<>();

        int length = ByteBuffer.wrap(recordsLength).getInt();
        for (int i = 0; i < length; i++) {
            KafkaRecord kafkaRecord = new KafkaRecord.Builder()
                    .setLength(readVarint(in))
                    .setAttributes(in.readNBytes(1))
                    .setTimestampDelta(readVarint(in))
                    .setOffsetDelta(readVarint(in))
                    .setKeyLength(readVarint(in))
                    .setKey(null)
                    .setValueLength(readVarint(in))
                    .setValue(getKafkaRecordValue(in)).build();

            records.add(kafkaRecord);
        }

        return records;

    }

    private KafkaRecordValue getKafkaRecordValue(InputStream in) throws IOException {
        byte[] frameVersion = in.readNBytes(1);
        byte[] type = in.readNBytes(1);

        KafkaRecordValue kafkaRecordValue = KafkaValueRecordFactory.createValueRecord(type, frameVersion, in);

        return kafkaRecordValue;
    }

    public List<KafkaRecordBatch> getKafkaRecordBatch() {
        return kafkaRecordBatchs;
    }

    public byte[] getPartitionTopicName() {
        return partitionTopicName;
    }

    public byte[] getPartitionIndex() {
        return partitionIndex;
    }

    public byte[] getPartitionTopicNameLength() {
        return partitionTopicNameLength;
    }

    public byte[] getContent() {
        return content;
    }

    public byte[] getTopicArrayLength() {
        return topicArrayLength;
    }

    public byte[] getPartitionLimits() {
        return partitionLimits;
    }

    public byte[] getCursor() {
        return cursor;
    }

    public byte[] getTopicNameLength() {
        return topicNameLength;
    }

    public byte[] getTopicName() {
        return topicName;
    }


    public static class Builder extends KafkaRequest.Builder<Builder> {
        private final DescribeTopicPartitionsRequest request;

        public Builder() {
            super(new DescribeTopicPartitionsRequest());
            this.request = (DescribeTopicPartitionsRequest) this.kafkaRequest;
        }

        public Builder setTopicNameLength(byte[] topicNameLength) {
            request.topicNameLength = topicNameLength;
            return this;
        }

        public Builder setTopicName(byte[] topicName) {
            request.topicName = topicName;
            return this;
        }

        public Builder setArrayLength(byte[] arrayLength) {
            request.topicArrayLength = arrayLength;
            return this;
        }

        public Builder setContent(byte[] content) {
            request.content = content;
            return this;
        }

        public Builder setPartitionLimits(byte[] partitionLimits) {
            request.partitionLimits = partitionLimits;
            return this;
        }

        public Builder setCursor(byte[] cursor) {
            request.cursor = cursor;
            return this;
        }

        public Builder setPartitionTopicName(byte[] partitionTopicName) {
            request.partitionTopicName = partitionTopicName;
            return this;
        }

        public Builder setPartitionIndex(byte[] partitionIndex) {
            request.partitionIndex = partitionIndex;
            return this;
        }

        public Builder setPartitionTopicNameLength(byte[] partitionTopicNameLength) {
            request.partitionTopicNameLength = partitionTopicNameLength;
            return this;
        }


        public Builder from(ApiVersionRequest apiRequest) {
            this.setMessageSize(apiRequest.getMessageSize())
                    .setRequestApiKey(apiRequest.getRequestApiKey())
                    .setRequestApiVersion(apiRequest.getRequestApiVersion())
                    .setCorrelationId(apiRequest.getCorrelationId());
            return this;
        }

        @Override
        public DescribeTopicPartitionsRequest build() {
            return request;
        }
    }
}
