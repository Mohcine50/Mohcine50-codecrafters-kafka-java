package lib;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static byte[] readVarintBytes(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }
            buffer.write(b);

            if ((b & 0x80) == 0) { // MSB is 0, stop reading
                break;
            }
        }
        return buffer.toByteArray();
    }

}
