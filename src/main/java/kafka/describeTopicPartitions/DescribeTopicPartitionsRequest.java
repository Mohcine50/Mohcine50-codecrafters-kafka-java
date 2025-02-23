package kafka.describeTopicPartitions;

import kafka.apiVersion.ApiVersionRequest;
import kafka.request.KafkaRequest;
import kafka.request.RequestInterface;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

import static lib.Constants.KAFKA_METADATA_CLUSTER_LOG_FILE_PATH;
import static lib.Utils.*;

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
    private Cursor cursor;
    private byte[] partitionTopicName;
    private byte[] partitionIndex;
    private Integer partitionTopicNameLength;
    private List<KafkaRecordBatch> kafkaRecordBatches;

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

        this.kafkaRecordBatches = handleKafkaMetaDataCluster();


        var topicUUID = getTopicUUID(kafkaRecordBatches).get();

        // Correlation Id
        baos.write(this.getCorrelationId());
        // tag buffer
        baos.write(new byte[]{0});

        // Throttle Time
        baos.write(new byte[]{0, 0, 0, 0});

        // Array Length : The length of the topics array + 1
        baos.write(writeUnsignedVarint(ByteBuffer.wrap(topicArrayLength).get()));

        // Error Code: Error COde for partition
        baos.write(new byte[]{0, 0});
        // Topic Name Length
        baos.write(ByteBuffer.wrap(topicNameLength).get());
        // Topic Name Content
        baos.write(topicName);
        // Topic ID
        baos.write(topicUUID);
        // Is Internal
        baos.write(new byte[]{0});
        // Partitions Array
        var partitions = getPartitionsArrayForTopic(topicUUID, kafkaRecordBatches);
        byte partitionsSize = (byte) partitions.size();
        System.out.println("number of partitions: " + partitions.size() + " " + (byte) (partitionsSize + 1));

        baos.write(writeUnsignedVarint(partitionsSize + 1));
        for (int i = 0; i < partitionsSize; i++) {
            baos.write(new byte[]{0, 0});
            baos.write(partitions.get(i).getPartitionId());
            baos.write(partitions.get(i).getLeader());
            baos.write(partitions.get(i).getLeaderEpoch());
            baos.write(partitions.get(i).getReplicationLength());
            baos.write(partitions.get(i).getReplicaArray());
            baos.write(partitions.get(i).getInSyncReplicaArrayLength());
            baos.write(partitions.get(i).getInSyncReplicaArray());
            baos.write(partitions.get(i).getAddingReplicasCount());
            //baos.write(partitions.get(i).);
            baos.write(0);
        }
        // Topic Authorized Operations
        baos.write(new byte[]{0, 0, (byte) 0x0d, (byte) 0xf8});
        baos.write(0);
        if (!Cursor.isNull(cursor)) {
            baos.write(writeUnsignedVarint(cursor.getLength()));
            baos.write(cursor.getName());
            baos.write(0);
            baos.write(cursor.getPartitionId());
        } else {
            System.out.println("WEEEEEEEEEEEEE");
            baos.write(new byte[]{(byte) 0xff});

        }

        baos.write(0);

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        var data = baos.toByteArray();
        out.write(data);
        System.out.println(Arrays.toString(HexFormat.of().formatHex(data).split(" ")));

        out.flush();
    }


    private List<PartitionRecordValue> getPartitionsArrayForTopic(byte[] topicUUID, List<KafkaRecordBatch> batches) {

        return batches.stream()
                .flatMap(batch -> batch.getRecords().stream())
                .map(KafkaRecord::getValue)
                .filter(val -> val instanceof PartitionRecordValue)
                .map(val -> (PartitionRecordValue) val)
                .filter(partitionRecord -> Arrays.equals(partitionRecord.getTopicUUID(), topicUUID))
                .toList();
    }


    private Optional<byte[]> getTopicUUID(List<KafkaRecordBatch> batches) {
        return batches.stream()
                .flatMap(batch -> batch.getRecords().stream())
                .map(KafkaRecord::getValue)
                .filter(val -> val instanceof TopicRecordValue)
                .map(val -> (TopicRecordValue) val)
                .filter(topicRecord -> {
                    return Arrays.equals(topicRecord.getTopicName(), topicName);
                })
                .map(TopicRecordValue::getTopicUUID)
                .findFirst();
    }

    InputStream getMetaDataLogFileInputStream() throws FileNotFoundException {

        Path path = Path.of(KAFKA_METADATA_CLUSTER_LOG_FILE_PATH);


        return new FileInputStream(path.toFile());
    }

    public List<KafkaRecordBatch> handleKafkaMetaDataCluster() throws IOException {

        InputStream in = getMetaDataLogFileInputStream();

        List<KafkaRecordBatch> kb = new ArrayList<KafkaRecordBatch>();

        while (in.available() != 0) {
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
                    .setBaseSequence(in.readNBytes(4))
                    .build();

            // - Records Length: Records Length is a 4-byte big-endian integer indicating the number of records in this batch.

            byte[] recordsLength = in.readNBytes(4);
            kafkaRecordBatch.setRecordsLength(recordsLength);
            kafkaRecordBatch.setRecords(getKafkaRecords(in, recordsLength));
            kb.add(kafkaRecordBatch);

        }

        return kb;

    }

    private ArrayList<KafkaRecord> getKafkaRecords(InputStream in, byte[] recordsLength) throws IOException {

        ArrayList<KafkaRecord> records = new ArrayList<>();


        int length = ByteBuffer.wrap(recordsLength).getInt();


        for (int i = 0; i < length; i++) {

            KafkaRecord kafkaRecord = new KafkaRecord.Builder()
                    .setLength(getSignedVarInt(in))
                    .setAttributes(in.readNBytes(1))
                    .setTimestampDelta(getSignedVarInt(in))
                    .setOffsetDelta(getSignedVarInt(in))
                    .setKeyLength(getSignedVarInt(in))
                    .setKey(null)
                    .setValueLength(getSignedVarInt(in))
                    .setValue(KafkaValueRecordFactory.createValueRecord(in))
                    .setHeadersArrayCount(getUnsignedVarInt(in))
                    .build();

            records.add(kafkaRecord);
        }

        return records;

    }

    public List<KafkaRecordBatch> getKafkaRecordBatch() {
        return kafkaRecordBatches;
    }

    public byte[] getPartitionTopicName() {
        return partitionTopicName;
    }

    public byte[] getPartitionIndex() {
        return partitionIndex;
    }

    public int getPartitionTopicNameLength() {
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

    public Cursor getCursor() {
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

        public Builder setCursor(Cursor cursor) {
            request.cursor = cursor;
            return this;
        }

        public Builder setPartitionTopicName(byte[] partitionTopicName) {
            request.partitionTopicName = partitionTopicName;
            System.out.println("partitionTopicName: " + partitionTopicName);
            return this;
        }

        public Builder setPartitionIndex(byte[] partitionIndex) {
            request.partitionIndex = partitionIndex;
            System.out.println("partitionIndex: " + partitionIndex);
            return this;
        }

        public Builder setPartitionTopicNameLength(Integer partitionTopicNameLength) {
            request.partitionTopicNameLength = partitionTopicNameLength;
            System.out.println("partitionTopicNameLength: " + partitionTopicNameLength);
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
