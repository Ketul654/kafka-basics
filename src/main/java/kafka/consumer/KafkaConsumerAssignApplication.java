package kafka.consumer;

import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Consume messages from specific topics of partitions
 */
public class KafkaConsumerAssignApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerAssignApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Group id is needed for synchronous and asynchronous commit
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        TopicPartition multi1 = new TopicPartition(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, 1);
        TopicPartition single0 = new TopicPartition(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, 0);
        partitions.add(multi1);
        partitions.add(single0);

        kafkaConsumer.assign(partitions);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                    // Adding a bit delay intentionally for better logs
                    Thread.sleep(500);
                    // This will block till offset is committed
                    //kafkaConsumer.commitSync();

                    // This will asynchronously commit offset
                    kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            map.entrySet().stream().forEach(offsetMap -> LOGGER.info("Committed offset : {} : {}", offsetMap.getKey().toString(), offsetMap.getValue().toString()));
                        }
                    });
                }

            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming messages : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
