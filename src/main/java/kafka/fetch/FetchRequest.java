package kafka.fetch;

import kafka.request.KafkaRequest;
import kafka.request.RequestInterface;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class FetchRequest extends KafkaRequest implements RequestInterface {

    private final byte[] replicaId;
    private final byte[] maxWaitMs;
    private final byte[] minBytes;
    private final List<FetchTopic> topics;

    public FetchRequest(byte[] replicaId, byte[] maxWaitMs, byte[] minBytes, List<FetchTopic> topics) {
        this.replicaId = replicaId;
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.topics = topics;
    }

    public byte[] getReplicaId() {
        return replicaId;
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

    }
}

