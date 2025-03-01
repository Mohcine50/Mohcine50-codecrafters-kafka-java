package kafka.apiVersion;

import kafka.request.KafkaRequest;
import kafka.request.RequestInterface;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ApiVersionRequest extends KafkaRequest implements RequestInterface {

    private Socket clientSocket;
    private Map<String, byte[]> parseStreamMap = new HashMap<>();

    protected ApiVersionRequest() {
    }

    @Override
    public void handleRequest() {
        // Implementation here
    }

    @Override
    public void parseStream(InputStream in) {
        // Implementation here
    }

    @Override
    public void sendResponse(OutputStream out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Correlation Id
        baos.write(this.getCorrelationId());
        baos.write(new byte[]{0, 0});
        baos.write(4);
        baos.write(new byte[]{0, 18});
        baos.write(new byte[]{0, 0});
        baos.write(new byte[]{0, 4});
        baos.write(0);
        baos.write(new byte[]{0, 75});
        baos.write(new byte[]{0, 0});
        baos.write(new byte[]{0, 0});
        baos.write(0);
        baos.write(new byte[]{0, 1});
        baos.write(new byte[]{0, 0});
        baos.write(new byte[]{0, 16});
        baos.write(0);
        baos.write(new byte[]{0, 0, 0, 0});
        baos.write(0);

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(baos.toByteArray());
        out.flush();
    }

    public static class Builder extends KafkaRequest.Builder<Builder> {
        private final ApiVersionRequest request;

        public Builder() {
            super(new ApiVersionRequest());
            this.request = (ApiVersionRequest) this.kafkaRequest;
        }

        @Override
        public ApiVersionRequest build() {
            return request;
        }
    }
}
