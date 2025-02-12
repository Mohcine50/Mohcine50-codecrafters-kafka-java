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

                ByteArrayOutputStream baos = get_response();

                int message_size = baos.size();

                byte[] response = baos.toByteArray();

                byte[] size_byte = ByteBuffer.allocate(4).putInt(message_size).array();
                out.write(size_byte);
                out.write(response);
                out.flush();

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
            short length = dataInputStream.readShort();
            byte[] content = dataInputStream.readNBytes(length);

            dataInputStream.readNBytes(1); // tug buffer

            byte[] topicArrayLength = dataInputStream.readNBytes(1);
            byte[] topicNameLength = dataInputStream.readNBytes(1);
            byte[] topicName = dataInputStream.readNBytes(topicNameLength[0] - 1);

            dataInputStream.readNBytes(1); // tug buffer

            byte[] partitions = dataInputStream.readNBytes(4);

            byte[] cursor = dataInputStream.readNBytes(1);
            dataInputStream.readNBytes(1); // tug buffer

            System.out.println("heressssssssssss 1");
            parseStreamMap.put(CONTENT, content);
            System.out.println("heressssssssssss 2");
            parseStreamMap.put(TOPIC_ARRAY_LENGTH, topicArrayLength);
            System.out.println("heressssssssssss 3");
            parseStreamMap.put(TOPIC_NAME_LENGTH, topicNameLength);
            System.out.println("heressssssssssss 4");
            parseStreamMap.put(TOPIC_NAME, topicName);
            System.out.println("heressssssssssss 5");
            parseStreamMap.put(PARTITIONS, partitions);
            System.out.println("heressssssssssss 6");
            parseStreamMap.put(CURSOR, cursor);

            System.out.println("heressssssssssss");
        }
        byte[] remainingBytes = new byte[dataInputStream.available()];
        dataInputStream.readFully(remainingBytes);
    }

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     * Response for the DescribeTopicPartitions
     */
    private ByteArrayOutputStream get_response() {

        short apiVersion = ByteBuffer.allocate(2).wrap((byte[]) parseStreamMap.get(API_VERSION)).getShort();

        short apiKey = ByteBuffer.allocate(2).wrap((byte[]) parseStreamMap.get(API_KEY)).getShort();

        byte[] correlationId = parseStreamMap.get(CORRELATION_ID);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            baos.write(correlationId);

            if (apiVersion < 0 || apiVersion > 4) {
                switch (apiKey) {
                    case APIVERSIONS:
                        sendApiVersions(baos);
                        break;
                    case DESCRIBETOPICPARTITIONS:
                        sendDescribeTopicPartitionsResponse(baos);
                        break;
                    default:
                        throw new Error("API KEY NOT SUPPORTED");
                }

            } else {
                baos.write(new byte[] { 0, 35 });
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return baos;

    }

    void sendDescribeTopicPartitionsResponse(ByteArrayOutputStream baos)
            throws IOException {

        byte[] topicNameLength = parseStreamMap.get(TOPIC_NAME_LENGTH);
        byte[] topicName = parseStreamMap.get(TOPIC_NAME);
        byte[] arrayLength = parseStreamMap.get(TOPIC_ARRAY_LENGTH);

        baos.write(0); // tag buffer
        baos.write(new byte[] { 0, 0, 0, 0 }); // throtels time
        baos.write(arrayLength[0]); // tag buffer : Array length
        baos.write(new byte[] { 0, 3 });
        baos.write(topicNameLength[0]);
        baos.write(topicName);
        baos.write(new byte[16]);
        baos.write(0);
        baos.write(2);
        baos.write(new byte[] { 0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0 });
        baos.write(0);
        baos.write(0xFF);
        baos.write(0);

    }

    void sendApiVersions(ByteArrayOutputStream baos) throws IOException {

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
    }

}
