package kafka.consumer;

import kafka.listener.CustomRebalanceListener;
import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka Consumer to consume messages produced in transactions
 * Produce messages with KafkaTransactionProducerApplication and consume with this consumer
 */
public class KafkaTransactionConsumerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransactionConsumerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        /*
         Transaction related property
         If this is set to read_committed, it will read only committed messages. Other non-transactional messages will be consumed normally.
         If this is set to read_uncommitted, it will read all the messages including non-committed messages.
         */
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, KafkaConstants.ISOLATION_LEVEL_COMMITTED);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaConstants.MULTI_PARTITION_TOPIC_NAME);

        kafkaConsumer.subscribe(topics, new CustomRebalanceListener(kafkaConsumer));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming messages : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
