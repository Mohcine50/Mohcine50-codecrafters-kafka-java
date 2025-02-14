package kafka.describeTopicPartitions;

import kafka.apiVersion.ApiVersionRequest;
import kafka.request.KafkaRequest;
import kafka.request.RequestInterface;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

import static constants.Constants.KAFKA_METADATA_CLUSTER_LOG_FILE_PATH;

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
    private Map<String, byte[]> parseStreamMap = new HashMap<>();

    private byte[] content;
    private byte[] topicName;
    private byte[] topicNameLength;
    private byte[] topicArrayLength;
    private byte[] partitionLimits;
    private byte[] cursor;
    private byte[] partitionTopicName;
    private byte[] partitionIndex;
    private byte[] partitionTopicNameLength;


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
        System.out.println("AVAILABLE META-DATA CLUSTER: " + in.available());
        System.out.println("HEX" + HexFormat.of().formatHex(in.readAllBytes()));
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
