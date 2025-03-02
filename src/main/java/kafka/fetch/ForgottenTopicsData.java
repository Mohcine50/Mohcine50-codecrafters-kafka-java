package kafka.fetch;

import java.util.List;

public class ForgottenTopicsData {
    private final byte[] topicId;
    private final List<byte[]> partitions;

    public ForgottenTopicsData(byte[] topicId, List<byte[]> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public byte[] getTopicId() {
        return topicId;
    }

    public List<byte[]> getPartitions() {
        return partitions;
    }
}
