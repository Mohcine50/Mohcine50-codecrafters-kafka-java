import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){

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

         byte[] correlationId = get_correlation_id(in);



       out.write(new byte[]{0, 0, 0, 0});
       out.write(correlationId);
       out.write(new byte[]{0, 35});
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

    private static byte[] get_correlation_id(InputStream in) throws IOException {

        byte[] message_size = in.readNBytes(4);
        byte[] request_api_key = in.readNBytes(2);
        byte[] request_api_version = in.readNBytes(2);
        byte[] correlation_id = in.readNBytes(4);

        return correlation_id;
  }

}
