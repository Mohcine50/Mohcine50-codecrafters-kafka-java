package kafka.describeTopicPartitions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static lib.Utils.getUnsignedVarInt;

public class Cursor {

    private Integer length;
    private byte[] name;
    private byte[] partitionId;

    Cursor(Integer length, byte[] name, byte[] partitionId) {
        this.length = length;
        this.name = name;
        this.partitionId = partitionId;
    }

    public static Cursor nullCursor() {
        return new Cursor(null, null, null);
    }

    public static boolean isNull(Cursor cursor) {
        return cursor.getLength() == null && cursor.getName() == null && cursor.getPartitionId() == null;
    }

    public static Cursor New(InputStream in) throws IOException {

        System.out.println("0xFF");

        in.mark(1);  // Mark the current position (10-byte read limit)
        byte[] firstByte = in.readNBytes(1);

        byte check = ByteBuffer.wrap(firstByte).get();
        if (check != -1) {
            in.reset();  // Go back to the marked position
            int b = getUnsignedVarInt(in);
            byte[] name = in.readNBytes(b - 1);
            byte[] partitionId = in.readNBytes(4);
            return new Cursor(b, name, partitionId);
        }

        return nullCursor();
    }

    public byte[] getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(byte[] partitionId) {
        this.partitionId = partitionId;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getName() {
        return name;
    }

    public void setName(byte[] name) {
        this.name = name;
    }


}
