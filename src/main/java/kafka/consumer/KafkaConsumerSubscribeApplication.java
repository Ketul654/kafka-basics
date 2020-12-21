package kafka.consumer;

import kafka.data.Employee;
import kafka.serde.EmployeeDeserializer;
import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumerSubscribeApplication {
    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerSubscribeApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmployeeDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(KafkaConstants.SINGLE_PARTITION_TOPIC_NAME);

        KafkaConsumer<String, Employee> kafkaConsumer = new KafkaConsumer<String, Employee>(properties);
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {

                ConsumerRecords<String, Employee> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Employee> record : records) {
                    logger.info(String.format("Topic : %s, Partition : %s, Offset : %s, Key : %s, Value : %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception ex) {
            logger.error("Exception occurred while consuming employee data : ", ex);
        } finally {
            kafkaConsumer.close();
        }
    }
}
