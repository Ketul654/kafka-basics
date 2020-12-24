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

/**
 * Kafka producer with Transaction
 * This can provide end-to-end exactly once semantics
 */
public class KafkaTransactionalProducerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransactionalProducerApplication.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // This configs will help to make sure that producer will not produce duplicate messages
        properties.put(ProducerConfig.ACKS_CONFIG, KafkaConstants.ALL_BROKER_ACKS);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, KafkaConstants.ENABLE_IDEMPOTENCE);

        // Transaction configuration
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, KafkaConstants.TRANSACTIONAL_ID);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, KafkaConstants.TRANSACTION_TIMEOUT);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        kafkaProducer.initTransactions();
        try {
            for (int i = 0; i < 10; i++) {
                kafkaProducer.beginTransaction();
                for (int j = 0; j < 2; j++) {
                    ProducerRecord record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, String.format("Message %d with transaction %d:%s", j, i, UUID.randomUUID().toString()));
                    kafkaProducer.send(record);
                }
                LOGGER.info("Messages for transaction {} are sent but not committed", i);
                Thread.sleep(5000);
                kafkaProducer.commitTransaction();
                LOGGER.info("Messages are committed for transaction {}", i);
                Thread.sleep(5000);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
            kafkaProducer.abortTransaction();
        } finally {
            kafkaProducer.close();
        }
    }
}
