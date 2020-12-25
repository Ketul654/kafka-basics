package com.ketul.kafka.consumer;
import com.ketul.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * This will print latency between messages sent by producer and consumed by consumer
 * Start this consumer
 * Start Kafka Advance Producer with different acks, linger.ms, batch.size, compression.type and max.in.flight.requests.per.connection
 * Enable/Disable compression, enable/disable batching, enable/disable acks, increase/decrease linger.ms and batch.size and see the difference
 * Test this with synchronous producer and see the difference
 */
public class KafkaConsumerLatencyCalculatorApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerLatencyCalculatorApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaConstants.MULTI_PARTITION_TOPIC_NAME);
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                long currentTime = System.currentTimeMillis();
                records.records(KafkaConstants.MULTI_PARTITION_TOPIC_NAME)
                        .forEach(record -> LOGGER.info(String.format("Latency %s ms -> Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                                (currentTime - record.timestamp()), record.topic(), record.partition(), record.offset(), record.key(), record.value())));

            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while consuming messages : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
