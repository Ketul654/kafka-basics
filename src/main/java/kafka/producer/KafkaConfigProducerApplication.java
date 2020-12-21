package kafka.producer;

import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;

public class KafkaConfigProducerApplication {
    private static Logger logger = LoggerFactory.getLogger(KafkaConfigProducerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 40000); // this should be >= LINGER_MS_CONFIG + REQUEST_TIMEOUT_MS_CONFIG
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10000);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32000);
        properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "resolve_canonical_bootstrap_servers_only");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "ketul-producer");
        properties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 120000);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10000);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 600000);
        properties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 120000);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 200);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 30000);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tran-1");


        //sendMessage(properties, "0");
        sendMessage(properties, "all");
        //sendMessage(properties, "0");
        //sendMessage(properties, "1");

    }

    private static void sendMessage(Properties properties, String acks) {

        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        int numOfMessages = 100;
        ProducerRecord record;
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < numOfMessages; i++) {
                record = new ProducerRecord(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, String.format("Message with extra configs:%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
            logger.info("Took {} ms to publish {} messages to {} topic with acknowledgement {}",
                    System.currentTimeMillis() - startTime, numOfMessages, KafkaConstants.SINGLE_PARTITION_TOPIC_NAME, acks);
        } catch (Exception ex) {
            logger.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
