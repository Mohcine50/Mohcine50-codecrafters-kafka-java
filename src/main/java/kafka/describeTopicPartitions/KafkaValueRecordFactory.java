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

                var partitionId = in.readNBytes(4);
                var topicUUID = in.readNBytes(16);
                var replicaArrayLength = getUnsignedVarInt(in);
                var replica = CompactArray.from(in, replicaArrayLength);
                var inSyncReplicaArrayLength = getUnsignedVarInt(in);
                var inSyncReplica = CompactArray.from(in, inSyncReplicaArrayLength);
                var removingReplicasArrayLength = getUnsignedVarInt(in);
                var removingReplica = CompactArray.from(in, removingReplicasArrayLength);
                var addingReplicasCount = getUnsignedVarInt(in);
                var addingReplicas = CompactArray.from(in, addingReplicasCount);
                var leader = in.readNBytes(4);
                var leaderEpoch = in.readNBytes(4);
                var partitionEpoch = in.readNBytes(4);

                var directoriesArrayLength = getUnsignedVarInt(in);
                var directories = CompactArray.from(in, directoriesArrayLength, 16);


                return new PartitionRecordValue.Builder()
                        .frameVersion(frameVersion)
                        .type(type)
                        .version(version)
                        .partitionId(partitionId)
                        .topicUUID(topicUUID)
                        .replicaArrayLength(replicaArrayLength)
                        .replicaArray(replica)
                        .inSyncReplicaArrayLength(inSyncReplicaArrayLength)
                        .inSyncReplicaArray(inSyncReplica)
                        .removingReplicasArrayLength(removingReplicasArrayLength)
                        .removingReplicasCount(removingReplica)
                        .addingReplicasCount(addingReplicasCount)
                        .addingReplicaArray(addingReplicas)
                        .leader(leader)
                        .leaderEpoch(leaderEpoch)
                        .partitionEpoch(partitionEpoch)
                        .directoriesArrayLength(directoriesArrayLength)
                        .directoriesArray(directories)
                        .taggedFieldsCount(getUnsignedVarInt(in))
                        .build();

            default:
                throw new IllegalArgumentException("Invalid level: " + level);
        }
    }
}
