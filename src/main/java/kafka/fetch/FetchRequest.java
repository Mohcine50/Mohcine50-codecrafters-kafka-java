package kafka.fetch;

import kafka.describeTopicPartitions.CompactString;
import kafka.describeTopicPartitions.KafkaRecordBatch;
import kafka.request.RequestInterface;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static kafka.describeTopicPartitions.DescribeTopicPartitionsRequest.getTopicByUUID;
import static kafka.describeTopicPartitions.DescribeTopicPartitionsRequest.handleKafkaMetaDataCluster;
import static lib.Utils.writeUnsignedVarint;

public class FetchRequest implements RequestInterface {

    private final byte[] maxWaitMs;
    private final byte[] minBytes;
    private final byte[] isolationLevel;
    private final byte[] maxBytes;
    private final byte[] sessionId;
    private final byte[] sessionEpoch;
    private final List<FetchTopic> topics;
    private final List<ForgottenTopicsData> forgottenTopicsData;
    private final CompactString rackId;

    private final byte[] correlationId;
    private List<KafkaRecordBatch> kafkaRecordBatches;


    public FetchRequest(byte[] correlationId, byte[] maxWaitMs, byte[] minBytes, byte[] isolationLevel, byte[] maxBytes, byte[] sessionId, byte[] sessionEpoch, List<FetchTopic> topics, List<ForgottenTopicsData> forgottenTopicsData, CompactString rackId) {
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.isolationLevel = isolationLevel;
        this.maxBytes = maxBytes;
        this.sessionId = sessionId;
        this.sessionEpoch = sessionEpoch;
        this.topics = topics;
        this.forgottenTopicsData = forgottenTopicsData;
        this.rackId = rackId;
        this.correlationId = correlationId;
    }

    public byte[] getCorrelationId() {
        return correlationId;
    }

    public byte[] getIsolationLevel() {
        return isolationLevel;
    }

    public byte[] getMaxBytes() {
        return maxBytes;
    }

    public CompactString getRackId() {
        return rackId;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public byte[] getSessionEpoch() {
        return sessionEpoch;
    }


    public byte[] getMaxWaitMs() {
        return maxWaitMs;
    }

    public byte[] getMinBytes() {
        return minBytes;
    }

    public List<FetchTopic> getTopics() {
        return topics;
    }

    @Override
    public void handleRequest() {

    }

    @Override
    public void parseStream(InputStream in) {

    }

    @Override
    public void sendResponse(OutputStream out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        System.out.println("Sending response");

        this.kafkaRecordBatches = handleKafkaMetaDataCluster();


        // CorrelationId
        baos.write(this.getCorrelationId());
        // tag buffer
        baos.write(new byte[]{0});

        // Throttle Time
        baos.write(new byte[]{0, 0, 0, 0});

        // Error code
        baos.write(new byte[]{0, 0});

        // SessionId
        baos.write(new byte[]{0, 0, 0, 0});

        // Responses
        System.out.println(topics.size());
        baos.write(writeUnsignedVarint(topics.size() + 1));
        for (FetchTopic topic : topics) {
            var _topic = getTopicByUUID(kafkaRecordBatches, topic.getTopicId());

            if (_topic.isPresent()) {
                var data = _topic.get();
                baos.write(topic.getTopicId());
                baos.write(writeUnsignedVarint(topic.getPartitions().size() + 1));

                for (FetchPartition partition : topic.getPartitions()) {

                    String topicName = new String(data.getTopicName(), StandardCharsets.UTF_8);
                    int partitionIndex = ByteBuffer.wrap(partition.getPartitionId()).getInt();
                    TopicFile topicFile = TopicFile.New(topicName, partitionIndex);

                    List<byte[]> records = topicFile.getBatchRecords();

                    baos.write(partition.getPartitionId());
                    baos.write(new byte[]{0, 0});
                    baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                    baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                    baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                    baos.write((byte) 1);
                    baos.write(new byte[]{0, 0, 0, 0});
                    baos.write((byte) records.size() + 1);
                    for (byte[] record : records) {
                        baos.write(record);
                    }
                    baos.write((byte) 0);

                }
            } else {
                baos.write(topic.getTopicId());
                baos.write(writeUnsignedVarint(2));
                baos.write(new byte[]{0, 0, 0, 0});
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeShort(100);
                dos.flush();
                baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                baos.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
                baos.write((byte) 1);
                baos.write(new byte[]{0, 0, 0, 0});
                baos.write((byte) 1);
                baos.write((byte) 0);
            }
            baos.write((byte) 0);
        }


        // Tag Buffer
        baos.write((byte) 0);

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        var data = baos.toByteArray();
        out.write(data);

        out.flush();

    }

    public List<ForgottenTopicsData> getForgottenTopicsData() {
        return forgottenTopicsData;
    }
}

