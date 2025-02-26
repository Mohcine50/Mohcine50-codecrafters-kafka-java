package kafka.describeTopicPartitions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class CompactArray {

    private List<byte[]> array;

    CompactArray(List<byte[]> array) {
        this.array = array;
    }

    public static CompactArray from(InputStream inputStream, int length) throws IOException {
        List<byte[]> array = new ArrayList<>();
        for (int i = 0; i < length - 1; i++) {
            array.add(inputStream.readNBytes(4));
        }
        return new CompactArray(array);
    }

    public static CompactArray from(InputStream inputStream, int length, int numberOfBytes) throws IOException {
        List<byte[]> array = new ArrayList<>();
        for (int i = 0; i < length - 1; i++) {
            array.add(inputStream.readNBytes(numberOfBytes));
        }
        return new CompactArray(array);
    }

    public byte[] toBytes() throws IOException {
        var baos = new ByteArrayOutputStream();
        for (byte[] bytes : array) {
            baos.write(bytes);
        }
        return baos.toByteArray();
    }
}
