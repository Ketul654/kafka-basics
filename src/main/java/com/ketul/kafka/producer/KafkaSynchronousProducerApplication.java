package com.ketul.kafka.producer;

import com.ketul.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Message will be sent synchronously to topic
 * Make it asynchronous by removing future.get() call and check the difference
 */
public class KafkaSynchronousProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSynchronousProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 0; i < 10; i++) {
                String message = String.format("Synchronously sent message %d:%s", i, UUID.randomUUID().toString());
                Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, message));
                RecordMetadata metadata = future.get();
                LOGGER.info("Message {} has been sent on topic {} and partition {} with timestamp {}", message, metadata.topic(), metadata.partition(), metadata.timestamp());
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
