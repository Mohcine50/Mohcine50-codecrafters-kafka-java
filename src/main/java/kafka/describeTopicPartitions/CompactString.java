package kafka.describeTopicPartitions;

import java.io.IOException;
import java.io.InputStream;

public class CompactString {

    private byte[] value;

    CompactString(byte[] value) {
        this.value = value;
    }

    public static CompactString from(InputStream in, byte length) throws IOException {
        return new CompactString(in.readNBytes(length - 1));
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

}
