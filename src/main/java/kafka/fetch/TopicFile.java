package kafka.fetch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TopicFile {

    private final Path filePath;

    private TopicFile(Path filePath) {
        this.filePath = filePath;
    }

    public static TopicFile New(String topicName, int partitionIndex) {

        String fileName = String.format("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionIndex);

        return new TopicFile(Path.of(fileName));
    }


    public List<byte[]> getBatchRecords() throws IOException {
        InputStream inputStream = new BufferedInputStream(getMetaDataLogFileInputStream());

        List<byte[]> batchRecords = new ArrayList<>();

        while (inputStream.available() != 0) {
            inputStream.mark(12);
            inputStream.readNBytes(8);
            int batchLength = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
            inputStream.reset();
            byte[] batch = inputStream.readNBytes(batchLength + 12);
            batchRecords.add(batch);
        }
        return batchRecords;
    }


    InputStream getMetaDataLogFileInputStream() throws FileNotFoundException {

        return new FileInputStream(this.filePath.toFile());
    }

}
