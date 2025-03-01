package kafka.request;

import kafka.apiVersion.ApiVersionRequest;
import kafka.describeTopicPartitions.CompactString;
import kafka.describeTopicPartitions.Cursor;
import kafka.describeTopicPartitions.DescribeTopicPartitionsRequest;
import kafka.fetch.FetchPartition;
import kafka.fetch.FetchRequest;
import kafka.fetch.FetchTopic;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static lib.Constants.*;


public class RequestHandler {

    private Socket clientSocket;


    private DescribeTopicPartitionsRequest describeTopicPartitionsRequest;
    private ApiVersionRequest versionRequest;
    private FetchRequest fetchRequest;

    public RequestHandler(Socket socket) {
        this.clientSocket = socket;
    }

    @SuppressWarnings("static-access")
    public void handleRequest() {

        try {
            while (!clientSocket.isClosed()) {
                // From where we read the request
                OutputStream out = clientSocket.getOutputStream();
                InputStream in = clientSocket.getInputStream();
                // We parse the input from the Input Stream
                parseStream(in);

                sendResponse(out);

            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    @SuppressWarnings("static-access")
    private void parseStream(InputStream in) throws IOException {

        DataInputStream dataInputStream = new DataInputStream(in);

        byte[] message_size = dataInputStream.readNBytes(4);
        byte[] request_api_key = dataInputStream.readNBytes(2);
        byte[] request_api_version = dataInputStream.readNBytes(2);
        byte[] correlation_id = dataInputStream.readNBytes(4);

        short api_key = ByteBuffer.allocate(2).wrap(request_api_key).getShort();

        versionRequest = new ApiVersionRequest.Builder()
                .setRequestApiVersion(request_api_version)
                .setRequestApiKey(request_api_key)
                .setMessageSize(message_size)
                .setCorrelationId(correlation_id)
                .build();

        // Check if the request api version is for Desccribe topic partitions
        if (api_key == DESCRIBE_TOPIC_PARTITIONS) {

            // Client ID
            short clientIdLengthShort = dataInputStream.readShort();
            byte[] content = dataInputStream.readNBytes(clientIdLengthShort);

            dataInputStream.readNBytes(1); // tug buffer

            // DescribeTopicPartitions Request Body (v0)
            // Topics Array
            // COMPACT_ARRAY: Array Length + 1
            // Array Length
            byte[] topicArrayLength = dataInputStream.readNBytes(1);

            // Topic
            // The length of the topic name + 1
            List<CompactString> topics = new ArrayList<>();
            for (byte i = 0; i < ByteBuffer.wrap(topicArrayLength).get() - 1; i++) {
                // Topic Name Length
                byte[] topicNameLength = dataInputStream.readNBytes(1);
                // Topic Name
                topics.add(CompactString.from(in, ByteBuffer.wrap(topicNameLength).get()));
                dataInputStream.readNBytes(1); // tug buffer
            }

            // Response Partition Limit
            byte[] responsePartitionLimit = dataInputStream.readNBytes(4);

            // Cursor
            Cursor cursor = Cursor.New(in);

            dataInputStream.readNBytes(1);

            describeTopicPartitionsRequest = new DescribeTopicPartitionsRequest.Builder()
                    .from(versionRequest)
                    .setContent(content)
                    .setTopicsArrayLength(topicArrayLength)
                    .setPartitionTopicName(cursor.getName())
                    .setTopicsArray(topics)
                    .setCursor(cursor)
                    .setPartitionIndex(cursor.getPartitionId())
                    .setPartitionLimits(responsePartitionLimit)
                    .build();
        } else if (api_key == FETCH) {
            // Client ID
            short clientIdLengthShort = dataInputStream.readShort();
            byte[] content = dataInputStream.readNBytes(clientIdLengthShort);
            dataInputStream.readNBytes(1); // tug buffer

            byte[] replicaId = dataInputStream.readNBytes(4);
            byte[] maxWaitMs = dataInputStream.readNBytes(4);
            byte[] minBytes = dataInputStream.readNBytes(4);

            byte[] topicsAraLength = dataInputStream.readNBytes(4);

            int arrayLength = ByteBuffer.wrap(topicsAraLength).getInt();
            List<FetchTopic> fetchTopics = new ArrayList<>();

            if (arrayLength > 0) {

                for (int i = 0; i < arrayLength; i++) {

                    short topicNameLength = dataInputStream.readShort();
                    byte[] topicName = dataInputStream.readNBytes(topicNameLength);
                    var fetchPartition = new FetchPartition.Builder()
                            .partitionId(dataInputStream.readNBytes(4))
                            .fetchOffset(dataInputStream.readNBytes(8))
                            .maxBytes(dataInputStream.readNBytes(4))
                            .build();
                    fetchTopics.add(
                            new FetchTopic.Builder()
                                    .addPartition(fetchPartition)
                                    .name(topicName)
                                    .build()
                    );
                }
            }

            fetchRequest = new FetchRequest(replicaId, maxWaitMs, minBytes, fetchTopics);

        }
        byte[] remainingBytes = new byte[dataInputStream.available()];
        dataInputStream.readFully(remainingBytes);

    }

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     * Response for the DescribeTopicPartitions
     */
    private void sendResponse(OutputStream out) {

        short apiVersion = ByteBuffer.allocate(2).wrap(versionRequest.getRequestApiVersion()).getShort();

        short apiKey = ByteBuffer.allocate(2).wrap(versionRequest.getRequestApiKey()).getShort();

        try {
            if (apiVersion >= 0 && apiVersion <= 4) {
                switch (apiKey) {
                    case API_VERSIONS:
                        versionRequest.sendResponse(out);
                        break;
                    case DESCRIBE_TOPIC_PARTITIONS:
                        describeTopicPartitionsRequest.sendResponse(out);
                        break;
                    case FETCH:
                        fetchRequest.sendResponse(out);
                    default:
                        throw new Error("API KEY NOT SUPPORTED");
                }

            } else {
                sendErrorCode(out);
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

    }


    void sendErrorCode(OutputStream out) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        bos.write(versionRequest.getCorrelationId());
        bos.write(new byte[]{0, (byte) 35});
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(bos.toByteArray());
        out.flush();
    }

}
