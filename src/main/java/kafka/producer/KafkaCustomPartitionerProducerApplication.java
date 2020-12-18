package kafka.producer;

import kafka.partitioner.KeyValueHashPartitioner;
import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.UUID;

public class KafkaCustomPartitionerProducerApplication {
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
            for (int i = 0; i < 10; i++) {

                /*
                 This will use custom partitioner KeyValueHashPartitioner configured in PARTITIONER_CLASS_CONFIG
                */
                record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, Integer.toString(i), String.format("Message with Custom Partitioner:%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
