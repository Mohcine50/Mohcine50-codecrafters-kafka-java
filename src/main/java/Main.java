import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Main {


    final static String MESSAGE_SIZE = "MESSAGE_SIZE";
    final static String API_KEY = "API_KEY";
    final static String API_VERSION = "API_VERSION";
    final static String CORRELATION_ID = "CORRELATION_ID";


    public static void main(String[] args) {

        System.out.println("Program started");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            clientSocket = serverSocket.accept();

            OutputStream out = clientSocket.getOutputStream();

            InputStream in = clientSocket.getInputStream();

            Map<String, byte[]> parseStreamMap = parseStream(in);
            System.out.println(parseStreamMap);
            var api_version = ByteBuffer.wrap(parseStreamMap.get(API_VERSION)).getShort();

            if (api_version >= 0 || api_version <= 4) {


                ByteArrayOutputStream baos =  get_response(parseStreamMap.get(CORRELATION_ID));

                int message_size = baos.size();
                byte[] response = baos.toByteArray();

                byte[] size_byte = ByteBuffer.allocate(4).putInt(message_size).array();

                System.out.println(size_byte);
                System.out.println(message_size);
                System.out.println(new String(response, StandardCharsets.UTF_8));

                out.write(size_byte);
                out.write(response);



            } else out.write(new byte[]{0, 35});


            out.flush();


        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     */

    private static ByteArrayOutputStream get_response(byte[] cId){

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            baos.write(cId);
            baos.write(new byte[]{0,0});
            baos.write(2);
            baos.write(new byte[]{0,18});
            baos.write(new byte[]{0,0});
            baos.write(new byte[]{0,4});
            baos.write(0);
            baos.write(new byte[]{0,0,0,0});
            baos.write(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return baos;

    }

    private static Map<String, byte[]> parseStream(InputStream in) throws IOException {

        Map<String, byte[]> map = new HashMap<String, byte[]>();

        byte[] message_size = in.readNBytes(4);
        byte[] request_api_key = in.readNBytes(2);
        byte[] request_api_version = in.readNBytes(2);
        byte[] correlation_id = in.readNBytes(4);


        map.put(MESSAGE_SIZE, message_size);
        map.put(API_KEY, request_api_key);
        map.put(API_VERSION, request_api_version);
        map.put(CORRELATION_ID, correlation_id);

        return map;
    }

}
