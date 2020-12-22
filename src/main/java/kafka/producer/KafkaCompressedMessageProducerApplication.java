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

public class KafkaCompressedMessageProducerApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCompressedMessageProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Batching and compression configs
        properties.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConstants.LINGER_MS);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConstants.BATCH_SIZE);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConstants.GZIP_COMPRESSION_TYPE);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 0; i < 1000000; i++) {
                ProducerRecord record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, String.format("Compressed Message :%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            logger.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}