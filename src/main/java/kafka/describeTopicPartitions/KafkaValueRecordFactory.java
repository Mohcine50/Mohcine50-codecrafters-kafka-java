package kafka.describeTopicPartitions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static lib.Constants.*;
import static lib.Utils.readVarint;

public class KafkaValueRecordFactory {


    private KafkaValueRecordFactory() {
        // Private constructor to prevent instantiation
    }

    public static KafkaRecordValue createValueRecord(byte[] type, byte[] frameVersion, InputStream in) throws IOException {

        short level = ByteBuffer.allocate(2).wrap(type).getShort();

        byte[] version = in.readNBytes(1);
        int nameLength = readVarint(in);
        byte[] name = in.readNBytes(nameLength - 1);

        switch (level) {
            case FEATURE_LEVEL:

                return new FeatureLevelRecord.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .nameLength(readVarint(in))
                        .name(name)
                        .featureLevel(in.readNBytes(2))
                        .taggedFieldsCount(readVarint(in))
                        .build();

            case TOPIC_LEVEL:
                return new TopicRecordValue.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .nameLength(nameLength)
                        .topicName(name)
                        .topicUUID(in.readNBytes(16))
                        .taggedFieldsCount(readVarint(in))
                        .build();

            case PARTITION_LEVEL:
                return new PartitionRecordValue.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .partitionId(in.readNBytes(4))
                        .topicUUID(in.readNBytes(16))
                        .replicaArrayLength(readVarint(in))
                        .replicaArray(in.readNBytes(4))
                        .inSyncReplicaArrayLength(readVarint(in))
                        .inSyncReplicaArray(in.readNBytes(4))
                        .removingReplicasArrayLength(readVarint(in))
                        .addingReplicasCount(readVarint(in))
                        .leader(in.readNBytes(4))
                        .leaderEpoch(in.readNBytes(4))
                        .partitionEpoch(in.readNBytes(4))
                        .directoriesArray(in.readNBytes(16))
                        .taggedFieldsCount(readVarint(in))
                        .build();

            default:
                throw new IllegalArgumentException("Invalid level: " + level);
        }
    }
}
