package kafka.request;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface RequestInterface {

    void handleRequest();

    void parseStream(InputStream in);

    void sendResponse(OutputStream out) throws IOException;
}
