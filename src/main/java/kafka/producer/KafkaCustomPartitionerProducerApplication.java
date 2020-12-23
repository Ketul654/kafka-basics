package kafka.producer;

import kafka.partitioner.KeyValueHashPartitioner;
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
 * Custom KeyValueHashPartitioner partitioner will be used to decide which partition data should be published to
 */
public class KafkaCustomPartitionerProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCustomPartitionerProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Custom partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KeyValueHashPartitioner.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            ProducerRecord record;
            for (int i = 0; i < 1; i++) {

                /*
                 This will use custom partitioner KeyValueHashPartitioner configured in PARTITIONER_CLASS_CONFIG
                */
                record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, Integer.toString(i), String.format("Message with Custom Partitioner:%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);

            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
