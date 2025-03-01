package kafka.fetch;

import java.util.ArrayList;
import java.util.List;

public class FetchTopic {
    private final byte[] name;
    private final List<FetchPartition> partitions;

    private FetchTopic(Builder builder) {
        this.name = builder.name;
        this.partitions = builder.partitions;
    }

    public byte[] getName() {
        return name;
    }

    public List<FetchPartition> getPartitions() {
        return partitions;
    }

    public static class Builder {
        private byte[] name;
        private List<FetchPartition> partitions = new ArrayList<>();

        public Builder name(byte[] name) {
            this.name = name;
            return this;
        }

        public Builder addPartition(FetchPartition partition) {
            this.partitions.add(partition);
            return this;
        }

        public FetchTopic build() {
            return new FetchTopic(this);
        }
    }
}
