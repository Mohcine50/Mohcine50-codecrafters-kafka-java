package kafka.request;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
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

    public RequestHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public void handleRequest() {

        try {
            while (!clientSocket.isClosed()) {
                // From where we read the request
                OutputStream out = clientSocket.getOutputStream();
                InputStream in = clientSocket.getInputStream();
                // We parse the input from the Input Stream
                Map<String, Object> parseStreamMap = parseStream(in);
                ByteBuffer.allocate(2);
                short api_version = ByteBuffer.wrap((byte[]) parseStreamMap.get(API_VERSION)).getShort();
                short api_key = ByteBuffer.wrap((byte[]) parseStreamMap.get(API_KEY)).getShort();
                System.out.println(api_key + " " + api_version);
                byte topicNameLength = (byte) parseStreamMap.get(TOPIC_NAME_LENGTH);
                String topicName = (String) parseStreamMap.get(TOPIC_NAME);

                ByteArrayOutputStream baos = get_response((byte[]) parseStreamMap.get(CORRELATION_ID), api_version,
                        api_key, topicNameLength, topicName);

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
    private Map<String, Object> parseStream(InputStream in) throws IOException {

        DataInputStream dataInputStream = new DataInputStream(in);

        Map<String, Object> map = new HashMap<String, Object>();

        byte[] message_size = dataInputStream.readNBytes(4);
        byte[] request_api_key = dataInputStream.readNBytes(2);
        byte[] request_api_version = dataInputStream.readNBytes(2);
        byte[] correlation_id = dataInputStream.readNBytes(4);

        byte[] remainingBytes = null;
        short api_key = ByteBuffer.allocate(2).wrap(request_api_key).getShort();

        map.put(MESSAGE_SIZE, message_size);
        map.put(API_KEY, request_api_key);
        map.put(API_VERSION, request_api_version);
        map.put(CORRELATION_ID, correlation_id);

        System.out.println(api_key);
        // Check if the request api version is for Desccribe topic partitions
        if (api_key == DESCRIBETOPICPARTITIONS) {
            System.out.println("inside the id");
            Map<String, Object> desMap = parseDescriptionTopicPartitionMessage(dataInputStream);
            System.out.println("inside the id");
            map.putAll(desMap);
        }

        remainingBytes = new byte[dataInputStream.available()];
        dataInputStream.readFully(remainingBytes);

        return map;
    }

    Map<String, Object> parseDescriptionTopicPartitionMessage(DataInputStream dataInputStream) throws IOException {

        Map<String, Object> desMap = new HashMap<String, Object>();

        short length = dataInputStream.readShort();
        byte[] content = dataInputStream.readNBytes(length);
        byte tagBuffer = dataInputStream.readByte();
        short topicArrayLength = dataInputStream.readShort();
        byte topicNameLength = dataInputStream.readByte();
        String topicName = dataInputStream.readUTF();
        byte tagBuffer_1 = dataInputStream.readByte();
        byte[] partitions = dataInputStream.readNBytes(4);
        byte cursor = dataInputStream.readByte();
        byte tagBuffer_2 = dataInputStream.readByte();

        desMap.put(LENGTH, length);
        desMap.put(CONTENT, content);
        desMap.put(TOPIC_ARRAY_LENGTH, topicArrayLength);
        desMap.put(TOPIC_NAME_LENGTH, topicNameLength);
        desMap.put(TOPIC_NAME, topicName);
        desMap.put(PARTITIONS, partitions);
        desMap.put(CURSOR, cursor);

        return desMap;
    }

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     * Response for the DescribeTopicPartitions
     */
    private ByteArrayOutputStream get_response(byte[] cId, short api_version, short api_key, byte topicNameLength,
            String topicName) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            if (api_version < 0 || api_version > 4) {
                switch (api_key) {
                    case APIVERSIONS:
                        sendApiVersions(baos, cId);
                        break;
                    case DESCRIBETOPICPARTITIONS:
                        sendDescribeTopicPartitionsResponse(baos, cId, topicNameLength, topicName);
                        break;
                    default:
                        throw new Error("API KEY NOT SUPPORTED");
                }
            } else {
                baos.write(cId);
                baos.write(new byte[] { 0, 35 });
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return baos;

    }

    void sendDescribeTopicPartitionsResponse(ByteArrayOutputStream baos, byte[] cId, byte topicNameLength,
            String topicName)
            throws IOException {

        baos.write(cId);
        baos.write(new byte[] { 0, 0 });
        baos.write(0);
        baos.write(new byte[] { 0, 0, 0, 0 });
        baos.write(2);
        baos.write(new byte[] { 0, UNKNOWN_TOPIC_OR_PARTITION });
        baos.write(new byte[] { topicNameLength });
        baos.write(new String(topicName));
        baos.write(new byte[16]);
        baos.write(0);
        baos.write(1);
        baos.write(new byte[] { 0, 0, (byte) 0x0d, (byte) 0xf8 });
        baos.write(0);
        baos.write(new byte[] { (byte) 0xff });
        baos.write(0);

    }

    void sendApiVersions(ByteArrayOutputStream baos, byte[] cId) throws IOException {

        baos.write(cId);
        baos.write(new byte[] { 0, 0 });
        baos.write(2);
        baos.write(new byte[] { 0, 18 });
        baos.write(new byte[] { 0, 0 });
        baos.write(new byte[] { 0, 4 });
        baos.write(0);
        baos.write(new byte[] { 0, 0, 0, 0 });
        baos.write(0);
    }

}
