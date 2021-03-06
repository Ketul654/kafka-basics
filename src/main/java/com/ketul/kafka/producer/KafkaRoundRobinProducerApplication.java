package com.ketul.kafka.producer;

import com.ketul.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * Message will be published to partitions in round robin way
 */
public class KafkaRoundRobinProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRoundRobinProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            ProducerRecord record;
            for (int i = 0; i < 10; i++) {
                /*
                 This will use DefaultPartitioner(round-robin) as only topic and value are provided.
                */
                record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, String.format("Message with Round Robin Partitioner Strategy:%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
