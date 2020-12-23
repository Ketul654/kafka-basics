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
 * Consume messages from specific topics of partitions with advance settings
 */
public class KafkaConsumerAssignApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerAssignApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
         Group id is needed for synchronous and asynchronous commit
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        /*
         Setting minimum bytes to fetch. This can improve throughput but latency might get reduced.
         This is somewhat similar to batch.size in producer.
         Try different values and see what happens.
         */
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, KafkaConstants.FETCH_MIN_BYTES);

        /*
        Setting maximum bytes can be returned to consumer
         */
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, KafkaConstants.FETCH_MAX_BYTES);

        /*
         Setting maximum time out for fetching data.
         This is somewhat similar to linger.ms in producer.
         Try different values and see what happens.
         */
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, KafkaConstants.FETCH_MAX_WAIT_MS);

        /*
        Setting maximum bytes can be fetched per partition.
        Try different values and see what happens.
         */
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, KafkaConstants.MAX_PARTITION_FETCH_BYTES);

        /*
        Limiting maximum number of records returned in a single poll call
         */
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, KafkaConstants.MAX_POLL_RECORDS);

        /*
        Let it consume all the messages from topic and wait for connections.max.idle.ms
        Consumer will not be able to make fetch request after it stays ideal for connections.max.idle.ms. It will throw org.apache.kafka.common.errors.DisconnectException.
        If new messages are published to topic, it will again establish session and start fetching data
         */
        properties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, KafkaConstants.CONNECTIONS_MAX_IDLE_MS);

        /*
        Disabling auto topic creation while subscribing or assigning a topic/partition
        Enable auto topic creation and start consumer with new topic which does not exist. It is enabled by disable.
        Disable auto topic creation and do the same
         */
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
        TopicPartition multi1 = new TopicPartition(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, 1);
        TopicPartition single0 = new TopicPartition(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, 0);
        partitions.add(multi1);
        partitions.add(single0);

        kafkaConsumer.assign(partitions);

        /*
        Go to a specific offset
         */
        kafkaConsumer.seek(single0, 3950);

        LOGGER.info("Current position of {} : {}", single0.toString(), kafkaConsumer.position(single0));

        /*
        Go to end of partition
         */
        ArrayList<TopicPartition> partitionsToSeek = new ArrayList<>();
        partitionsToSeek.add(single0);
        kafkaConsumer.seekToEnd(partitionsToSeek);

        LOGGER.info("Current position of {} : {}", single0.toString(), kafkaConsumer.position(single0));

        /*
        Move to beginning
         */
        kafkaConsumer.seekToBeginning(partitionsToSeek);

        LOGGER.info("Current position of {} : {}", single0.toString(), kafkaConsumer.position(single0));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
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
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming messages : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
