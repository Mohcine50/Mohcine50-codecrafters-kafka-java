import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;

import kafka.request.RequestHandler;

public class Main {

    public static void main(String[] args) {

        System.out.println("Program started");

        final ExecutorService THREAD_POOL = newVirtualThreadPerTaskExecutor();

        ServerSocket serverSocket;
        int port = 9092;

        try {

            serverSocket = new ServerSocket(port);

            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            System.out.println("Accepting new connecion");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                RequestHandler requestHandler = new RequestHandler(clientSocket);

                THREAD_POOL.submit(() -> requestHandler.handleRequest());
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }

    }

}
