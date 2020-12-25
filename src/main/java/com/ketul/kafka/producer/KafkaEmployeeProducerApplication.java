package com.ketul.kafka.producer;

import com.ketul.kafka.utils.KafkaConstants;
import com.ketul.kafka.data.Department;
import com.ketul.kafka.data.Employee;
import com.ketul.kafka.interceptor.EmployeeProducerInterceptor;
import com.ketul.kafka.serde.EmployeeSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Random;

/**
 * This uses custom serializer EmployeeSerializer to serialize Employee object
 * EmployeeProducerInterceptor is used to limit age below 100 before sending data to brokers
 */
public class KafkaEmployeeProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCustomPartitionerProducerApplication.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerializer.class.getName());

        // Interceptor
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, EmployeeProducerInterceptor.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try {
            for (int i = 0; i < 200; i++) {
                Department department = Department.values()[new Random().nextInt(Department.values().length)];
                Employee employee = new Employee(String.format("Employee %d", i), i, department);
                ProducerRecord record = new ProducerRecord(KafkaConstants.EMPLOYEE_TOPIC_NAME, Integer.toString(i), employee);
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
        }
    }

}
