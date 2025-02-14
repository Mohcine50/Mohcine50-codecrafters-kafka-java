package kafka.request;

public class KafkaRequest {

    private byte[] messageSize;
    private byte[] requestApiKey;
    private byte[] requestApiVersion;
    private byte[] correlationId;

    protected KafkaRequest() {
        // Protected constructor to allow subclassing
    }

    public byte[] getMessageSize() {
        return messageSize;
    }

    protected void setMessageSize(byte[] messageSize) {
        this.messageSize = messageSize;
    }

    public byte[] getRequestApiKey() {
        return requestApiKey;
    }

    protected void setRequestApiKey(byte[] requestApiKey) {
        this.requestApiKey = requestApiKey;
    }

    public byte[] getRequestApiVersion() {
        return requestApiVersion;
    }

    protected void setRequestApiVersion(byte[] requestApiVersion) {
        this.requestApiVersion = requestApiVersion;
    }

    public byte[] getCorrelationId() {
        return correlationId;
    }

    protected void setCorrelationId(byte[] correlationId) {
        this.correlationId = correlationId;
    }

    public static class Builder<T extends Builder<T>> {
        protected KafkaRequest kafkaRequest;

        protected Builder(KafkaRequest kafkaRequest) {
            this.kafkaRequest = kafkaRequest;
        }

        public T setMessageSize(byte[] messageSize) {
            kafkaRequest.setMessageSize(messageSize);
            return self();
        }

        public T setRequestApiKey(byte[] requestApiKey) {
            kafkaRequest.setRequestApiKey(requestApiKey);
            return self();
        }

        public T setRequestApiVersion(byte[] requestApiVersion) {
            kafkaRequest.setRequestApiVersion(requestApiVersion);
            return self();
        }

        public T setCorrelationId(byte[] correlationId) {
            kafkaRequest.setCorrelationId(correlationId);
            return self();
        }

        public KafkaRequest build() {
            return kafkaRequest;
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }
    }
}
