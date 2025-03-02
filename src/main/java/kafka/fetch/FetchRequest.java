package kafka.fetch;

import kafka.describeTopicPartitions.CompactString;
import kafka.request.RequestInterface;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

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
        baos.write((byte) 0);


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

