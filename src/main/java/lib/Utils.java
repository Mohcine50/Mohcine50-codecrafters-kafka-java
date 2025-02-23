package lib;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static int readUnsignedVarint(InputStream input) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = input.read()) & 0x80) != 0) {
            if (b == -1) {
                throw new IOException("Unexpected end of stream");
            }
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28)
                throw illegalVarintException(value);
        }
        value |= b << i;
        return value;
    }


    private static IllegalArgumentException illegalVarintException(int value) {
        throw new IllegalArgumentException("Varint is too long, the most significant bit in the 5th byte is set, " +
                "converted value: " + Integer.toHexString(value));
    }

    public static int getUnsignedVarInt(InputStream inputStream) throws IOException {
        return readUnsignedVarint(inputStream);
    }

    public static int getSignedVarInt(InputStream inputStream) throws IOException {
        int value = readUnsignedVarint(inputStream);
        return (value >>> 1) ^ -(value & 1);
    }

    public static byte[] writeUnsignedVarint(int value) throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        while ((value & 0xffffff80) != 0L) {
            byte b = (byte) ((value & 0x7f) | 0x80);
            outputStream.write(b);
            value >>>= 7;
        }
        outputStream.write((byte) value);

        return outputStream.toByteArray();
    }
}
