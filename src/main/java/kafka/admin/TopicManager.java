package kafka.admin;

import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Admin Client to create, describe and delete topic and delete records from partition
 */
public class TopicManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicManager.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        AdminClient adminClient = KafkaAdminClient.create(properties);

        // Creating Topic
        String topicName = "temp";
        int partitions = 1;
        short replicationFactor = 3;
        NewTopic tempTopic = new NewTopic(topicName, partitions, replicationFactor);
        ArrayList<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(tempTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        createTopicsResult.all().get();
        LOGGER.info("Successfully created {} topic", topicName);

        // Describe Topic
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        LOGGER.info("Topic description : {} ", topicDescriptionMap);

        // Delete Topic
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
        deleteTopicsResult.all().get();
        LOGGER.info("Successfully deleted {} topic", topicName);

        // Delete Record
        int offset = 100000;
        TopicPartition topicPartition = new TopicPartition(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, 0);
        RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(offset);
        Map<TopicPartition, RecordsToDelete> toDeleteMap = new HashMap<>();
        toDeleteMap.put(topicPartition, recordsToDelete);
        DeleteRecordsResult deleteRecordsResult = adminClient.deleteRecords(toDeleteMap);
        deleteRecordsResult.all().get();
        LOGGER.info("Records are deleted from {} topic before offset {}", KafkaConstants.MULTI_PARTITION_TOPIC_NAME, offset);

    }
}
