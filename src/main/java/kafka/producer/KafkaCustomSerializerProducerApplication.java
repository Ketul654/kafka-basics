package kafka.producer;

import kafka.data.Department;
import kafka.data.Employee;
import kafka.serde.EmployeeSerializer;
import kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Random;

public class KafkaCustomSerializerProducerApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCustomPartitionerProducerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 0; i < 10; i++) {
                Department department = Department.values()[new Random().nextInt(Department.values().length)];
                Employee employee = new Employee(String.format("Employee %d", i), i, department);
                ProducerRecord record = new ProducerRecord(KafkaConstants.EMPLOYEE_TOPIC_NAME, Integer.toString(i), employee);
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            logger.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }

}
