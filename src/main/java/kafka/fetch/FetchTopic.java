package kafka.fetch;

import java.util.List;

public class FetchTopic {
    private final byte[] topicId;
    private final List<FetchPartition> partitions;

    public FetchTopic(byte[] topicId, List<FetchPartition> partitions) {
        this.topicId = topicId;
        this.partitions = partitions;
    }

    public byte[] getTopicId() {
        return topicId;
    }

    public List<FetchPartition> getPartitions() {
        return partitions;
    }


}
