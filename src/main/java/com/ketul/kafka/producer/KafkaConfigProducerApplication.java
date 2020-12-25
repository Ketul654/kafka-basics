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
 * Play around with majority of important producer configurations
 */
public class KafkaConfigProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigProducerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Other configs
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConstants.GZIP_COMPRESSION_TYPE);
        properties.put(ProducerConfig.RETRIES_CONFIG, KafkaConstants.RETRIES);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, KafkaConstants.RETRY_BACKOFF_MS);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, KafkaConstants.DELIVERY_TIMEOUT_MS); // this should be >= LINGER_MS_CONFIG + REQUEST_TIMEOUT_MS_CONFIG
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, KafkaConstants.REQUEST_TIMEOUT_MS);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConstants.LINGER_MS);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConstants.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConstants.BATCH_SIZE);
        properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, KafkaConstants.CLIENT_DNS_LOOKUP);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, KafkaConstants.CONNECTIONS_MAX_IDLE_MS);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, KafkaConstants.MAX_BLOCK_MS);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, KafkaConstants.MAX_REQUEST_SIZE);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, KafkaConstants.ENABLE_IDEMPOTENCE);
        properties.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, KafkaConstants.METADATA_MAX_IDLE);
        properties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, KafkaConstants.METADATA_MAX_AGE_MS);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, KafkaConstants.RECONNECT_BACKOFF_MAX_MS);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, KafkaConstants.RECONNECT_BACKOFF_MS);
        //properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, KafkaConstants.TRANSACTION_TIMEOUT);
        //properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, KafkaConstants.TRANSACTIONAL_ID);


        //sendMessage(properties, "0");
        sendMessage(properties, "all");
        //sendMessage(properties, "0");
        //sendMessage(properties, "1");

    }

    private static void sendMessage(Properties properties, String acks) {

        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        int numOfMessages = 100;
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < numOfMessages; i++) {
                ProducerRecord record = new ProducerRecord(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, String.format("Message with extra configs:%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
            LOGGER.info("Took {} ms to publish {} messages to {} topic with acknowledgement {}",
                    System.currentTimeMillis() - startTime, numOfMessages, KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, acks);
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
