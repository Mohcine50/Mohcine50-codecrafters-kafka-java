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

import static constants.Constants.API_VERSION;
import static constants.Constants.CORRELATION_ID;
import static constants.Constants.DESCRIBETOPICPARTITIONS;
import static constants.Constants.MESSAGE_SIZE;
import static constants.Constants.APIVERSIONS;
import static constants.Constants.API_KEY;
import static constants.Constants.REMAINING_BYTES;;

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
                Map<String, byte[]> parseStreamMap = parseStream(in);
                ByteBuffer.allocate(2);
                short api_version = ByteBuffer.wrap(parseStreamMap.get(API_VERSION)).getShort();

                ByteArrayOutputStream baos = get_response(parseStreamMap.get(CORRELATION_ID), api_version);

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

    private Map<String, byte[]> parseStream(InputStream in) throws IOException {

        DataInputStream dataInputStream = new DataInputStream(in);

        Map<String, byte[]> map = new HashMap<String, byte[]>();

        byte[] message_size = dataInputStream.readNBytes(4);
        byte[] request_api_key = dataInputStream.readNBytes(2);
        byte[] request_api_version = dataInputStream.readNBytes(2);
        byte[] correlation_id = dataInputStream.readNBytes(4);

        byte[] remainingBytes = new byte[dataInputStream.available()];
        dataInputStream.readFully(remainingBytes);

        map.put(MESSAGE_SIZE, message_size);
        map.put(API_KEY, request_api_key);
        map.put(API_VERSION, request_api_version);
        map.put(CORRELATION_ID, correlation_id);
        map.put(REMAINING_BYTES, remainingBytes);

        return map;
    }

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     * Response for the DescribeTopicPartitions
     */
    private ByteArrayOutputStream get_response(byte[] cId, short api_version) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {

            switch (api_version) {
                case APIVERSIONS:
                    sendApiVersions(baos, cId);
                    break;
                case DESCRIBETOPICPARTITIONS:
                    sendDescribeTopicPartitionsResponse(baos, cId);
                    break;
                default:
                    baos.write(cId);
                    baos.write(new byte[] { 0, 35 });
                    break;
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

        return baos;

    }

    void sendDescribeTopicPartitionsResponse(ByteArrayOutputStream baos, byte[] cId) throws IOException {

        baos.write(cId);
        baos.write(new byte[] { 0, 0 });
        baos.write(0);
        baos.write(new byte[] { 0, 0, 0, 0 });
        baos.write(2);
        baos.write(new byte[] { 0, 3 });
        baos.write(new byte[] { 0, 0 });
        baos.write(new byte[] { 0, 0 });
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
