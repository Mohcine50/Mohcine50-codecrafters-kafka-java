package kafka.request;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Base64.Decoder;

import static constants.Constants.API_VERSION;
import static constants.Constants.CONTENT;
import static constants.Constants.CORRELATION_ID;
import static constants.Constants.CURSOR;
import static constants.Constants.DESCRIBETOPICPARTITIONS;
import static constants.Constants.LENGTH;
import static constants.Constants.MESSAGE_SIZE;
import static constants.Constants.PARTITIONS;
import static constants.Constants.TOPIC_ARRAY_LENGTH;
import static constants.Constants.TOPIC_NAME;
import static constants.Constants.TOPIC_NAME_LENGTH;
import static constants.Constants.APIVERSIONS;
import static constants.Constants.API_KEY;
import static constants.Constants.UNKNOWN_TOPIC_OR_PARTITION;

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

public class RequestHandler {

    private Socket clientSocket;
    private Map<String, byte[]> parseStreamMap = new HashMap<String, byte[]>();

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

        parseStreamMap.put(MESSAGE_SIZE, message_size);
        parseStreamMap.put(API_KEY, request_api_key);
        parseStreamMap.put(API_VERSION, request_api_version);
        parseStreamMap.put(CORRELATION_ID, correlation_id);

        // Check if the request api version is for Desccribe topic partitions
        if (api_key == DESCRIBETOPICPARTITIONS) {

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
            byte[] topicNameLength = dataInputStream.readNBytes(1);
            // Topic Name
            byte[] topicName = dataInputStream.readNBytes(topicNameLength[0] - 1);

            dataInputStream.readNBytes(1); // tug buffer

            // Response Partition Limit
            byte[] responsePartitionLimit = dataInputStream.readNBytes(4);

            // Cursor
            byte[] cursor = dataInputStream.readNBytes(1);

            dataInputStream.readNBytes(1); // tug buffer

            parseStreamMap.put(CONTENT, content);
            parseStreamMap.put(TOPIC_ARRAY_LENGTH, topicArrayLength);
            parseStreamMap.put(TOPIC_NAME_LENGTH, topicNameLength);
            parseStreamMap.put(TOPIC_NAME, topicName);
            parseStreamMap.put(PARTITIONS, responsePartitionLimit);
            parseStreamMap.put(CURSOR, cursor);

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

        short apiVersion = ByteBuffer.allocate(2).wrap((byte[]) parseStreamMap.get(API_VERSION)).getShort();

        short apiKey = ByteBuffer.allocate(2).wrap((byte[]) parseStreamMap.get(API_KEY)).getShort();

        try {
            if (apiVersion >= 0 && apiVersion <= 4) {
                switch (apiKey) {
                    case APIVERSIONS:
                        sendApiVersions(out);
                        break;
                    case DESCRIBETOPICPARTITIONS:
                        sendDescribeTopicPartitionsResponse(out);
                        break;
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

    void sendDescribeTopicPartitionsResponse(OutputStream out)
            throws IOException {

        byte[] topicNameLength = parseStreamMap.get(TOPIC_NAME_LENGTH);
        byte[] topicName = parseStreamMap.get(TOPIC_NAME);
        byte[] arrayLength = parseStreamMap.get(TOPIC_ARRAY_LENGTH);
        byte[] correlationId = parseStreamMap.get(CORRELATION_ID);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Correlation Id
        baos.write(correlationId);
        // tag buffer
        baos.write(new byte[] { 0 });

        // Throttle Time
        baos.write(new byte[] { 0, 0, 0, 0 });

        // Array Length : The length of the topics array + 1
        baos.write(arrayLength);

        // Error Code
        baos.write(new byte[] { 0, 3 });
        // Topic Name Length
        baos.write(topicNameLength);
        // Topic Name Content
        baos.write(topicName);
        // Topic ID
        baos.write(new byte[16]);
        // Is Internal
        baos.write(new byte[] { 0 });
        // Partitions Array
        baos.write(new byte[] { 1 });
        // Topic Authorized Operations
        baos.write(new byte[] { 0, 0, (byte) 0x0d, (byte) 0xf8 });
        baos.write(new byte[] { 0 });
        baos.write(0xFF);
        baos.write(new byte[] { 0 });

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(baos.toByteArray());
        out.flush();

    }

    void sendApiVersions(OutputStream out) throws IOException {

        byte[] correlationId = parseStreamMap.get(CORRELATION_ID);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Correlation Id
        baos.write(correlationId);
        baos.write(new byte[] { 0, 0 });
        baos.write(3);
        baos.write(new byte[] { 0, 18 });
        baos.write(new byte[] { 0, 0 });
        baos.write(new byte[] { 0, 4 });
        baos.write(0);
        baos.write(new byte[] { 0, 75 });
        baos.write(new byte[] { 0, 0 });
        baos.write(new byte[] { 0, 0 });
        baos.write(0);
        baos.write(new byte[] { 0, 0, 0, 0 });
        baos.write(0);

        int size = baos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(baos.toByteArray());
        out.flush();
    }

    void sendErrorCode(OutputStream out) throws IOException {

        byte[] correlationId = parseStreamMap.get(CORRELATION_ID);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        bos.write(correlationId);
        bos.write(new byte[] { 0, (byte) 35 });
        int size = bos.size();
        out.write(ByteBuffer.allocate(4).putInt(size).array());
        out.write(bos.toByteArray());
        out.flush();
    }

}
