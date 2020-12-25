package com.ketul.kafka.consumergroup;

import com.ketul.kafka.utils.KafkaConstants;
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

/**
 * Consumer 0 of consumer group
 *
 * Start all the consumers from consumer group. Publish messages on subscribed topics. See how consumers consume messages.
 * Kill consumer and see how re-balances occur.
 * Add more consumers in consumer group and see what happens.
 *
 * Maximum number of consumers in a group = number of partitions in a topic. Extra consumer will be ideal.
 *
 * i.e. if 4 consumers are subscribed to a topic with partitions 3, one consumer will be ideal.
 * if 2 consumers are subscribed to a topic with partitions 3, one consumer will consume from 2 partitions and other will from 1 partition.
 */
public class KafkaConsumerGroupApplication0 {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerGroupApplication0.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER_GROUP_1_ID);

        /*
        Limiting maximum delay between two poll calls. If that exceeds, consumer will be considered fail and re-balance will happen.
         */
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, KafkaConstants.MAX_POLL_INTERVAL_MS);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME);
        topics.add(KafkaConstants.MULTI_PARTITION_TOPIC_NAME);

        kafkaConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }

                /*
                Sleeping to test max.poll.interval.ms
                 */
                Thread.sleep(KafkaConstants.MAX_POLL_INTERVAL_MS + 1000);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming messages : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
