package kafka.describeTopicPartitions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static lib.Constants.*;
import static lib.Utils.getUnsignedVarInt;

public class KafkaValueRecordFactory {


    private KafkaValueRecordFactory() {
        // Private constructor to prevent instantiation
    }

    public static KafkaRecordValue createValueRecord(InputStream in) throws IOException {


        byte[] frameVersion = in.readNBytes(1);
        byte[] type = in.readNBytes(1);

        byte level = ByteBuffer.wrap(type).get();

        System.out.println("level  " + level);

        byte[] version = in.readNBytes(1);


        switch (level) {
            case FEATURE_LEVEL:
                int nameLength = getUnsignedVarInt(in);
                byte[] name = in.readNBytes(nameLength - 1);
                return new FeatureLevelRecord.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .nameLength(nameLength)
                        .name(name)
                        .featureLevel(in.readNBytes(2))
                        .taggedFieldsCount(getUnsignedVarInt(in))
                        .build();

            case TOPIC_LEVEL:
                int topicNameLength = getUnsignedVarInt(in);
                byte[] topicName = in.readNBytes(topicNameLength - 1);
                return new TopicRecordValue.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .nameLength(topicNameLength)
                        .topicName(topicName)
                        .topicUUID(in.readNBytes(16))
                        .taggedFieldsCount(getUnsignedVarInt(in))
                        .build();

            case PARTITION_LEVEL:
                return new PartitionRecordValue.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .partitionId(in.readNBytes(4))
                        .topicUUID(in.readNBytes(16))
                        .replicaArrayLength(getUnsignedVarInt(in))
                        .replicaArray(in.readNBytes(4))
                        .inSyncReplicaArrayLength(getUnsignedVarInt(in))
                        .inSyncReplicaArray(in.readNBytes(4))
                        .removingReplicasArrayLength(getUnsignedVarInt(in))
                        .addingReplicasCount(getUnsignedVarInt(in))
                        .leader(in.readNBytes(4))
                        .leaderEpoch(in.readNBytes(4))
                        .partitionEpoch(in.readNBytes(4))
                        .directoriesArrayLength(getUnsignedVarInt(in))
                        .directoriesArray(in.readNBytes(16))
                        .taggedFieldsCount(getUnsignedVarInt(in))
                        .build();

            default:
                throw new IllegalArgumentException("Invalid level: " + level);
        }
    }
}
