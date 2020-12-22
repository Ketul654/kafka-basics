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

public class KafkaDirectProducerApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDirectProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        int partitionId = 1;
        try {
            ProducerRecord record;
            for (int i = 0; i < 10; i++) {
                /*
                 This will produce directly to provided valid partition as it is provided in producer record
                */
                record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, partitionId, Integer.toString(i), String.format("Message to direct partition %d:%s", partitionId, UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            logger.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }
}
