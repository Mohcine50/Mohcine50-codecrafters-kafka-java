import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Main {

    final static String MESSAGE_SIZE = "MESSAGE_SIZE";
    final static String API_KEY = "API_KEY";
    final static String API_VERSION = "API_VERSION";
    final static String CORRELATION_ID = "CORRELATION_ID";
    final static String REMAINING_BYTES = "REMAINING_BYTES";
    
    public static void main(String[] args) {

        System.out.println("Program started");

        ServerSocket serverSocket ;
        Socket clientSocket = null;
        int port = 9092;
        
      


        
        try {

            serverSocket = new ServerSocket(port);

            serverSocket.setReuseAddress(true);
                // Wait for connection from client.
                System.out.println("Accepting new connecion");
                clientSocket = serverSocket.accept();
                


                while (true) {
                    handleRequest(clientSocket);
                }
               
            
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

    private static void handleRequest(Socket clientSocket) throws IOException{

                        // Where we create the response
                        OutputStream out = clientSocket.getOutputStream();
                        // From where we read the request
                        InputStream in = clientSocket.getInputStream();
        //We parse the input from the Input Stream
        Map<String, byte[]> parseStreamMap = parseStream(in);
        short api_version = ByteBuffer.allocate(2).wrap(parseStreamMap.get(API_VERSION)).getShort();

        ByteArrayOutputStream baos = get_response(parseStreamMap.get(CORRELATION_ID), api_version);

        int message_size = baos.size();
        byte[] response = baos.toByteArray();

        byte[] size_byte = ByteBuffer.allocate(4).putInt(message_size).array();
        out.write(size_byte);
        out.write(response);
        out.flush();
    }
    

    /*
     * Response for the ApiVersion 18 on V4
     * error_code [api_keys] throttle_time_ms TAG_BUFFER
     */
    private static ByteArrayOutputStream get_response(byte[] cId, short api_version) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            if (api_version == 4) {

                baos.write(cId);
                baos.write(new byte[]{0, 0});
                baos.write(2);
                baos.write(new byte[]{0, 18});
                baos.write(new byte[]{0, 0});
                baos.write(new byte[]{0, 4});
                baos.write(0);
                baos.write(new byte[]{0, 0, 0, 0});
                baos.write(0);
            } else {
                baos.write(cId);
                baos.write(new byte[]{0, 35});
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return baos;

    }

    private static Map<String, byte[]> parseStream(InputStream in) throws IOException {

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

}
