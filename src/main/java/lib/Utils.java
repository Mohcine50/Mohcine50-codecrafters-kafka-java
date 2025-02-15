package lib;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static int readVarint(InputStream in) throws IOException {
        int value = 0;
        int shift = 0;
        int b;

        while (true) {
            b = in.read();
            if (b == -1) {
                throw new EOFException("Unexpected end of stream");
            }

            // Add the lower 7 bits of the byte to the value
            value |= (b & 0x7F) << shift;

            // If the MSB is 0, this is the last byte
            if ((b & 0x80) == 0) {
                break;
            }

            // Shift by 7 bits for the next byte
            shift += 7;

            // Prevent overflow (Varints are typically limited to 32 or 64 bits)
            if (shift >= 32) {
                throw new IOException("Varint too large");
            }
        }

        return value;
    }

}
