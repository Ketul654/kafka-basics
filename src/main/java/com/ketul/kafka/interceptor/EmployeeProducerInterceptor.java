package com.ketul.kafka.interceptor;

import com.ketul.kafka.data.Employee;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Message can be modified here before it gets published to broker
 */
public class EmployeeProducerInterceptor implements ProducerInterceptor<String, Employee> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeProducerInterceptor.class);
    private int messagesSent = 0;
    private int messagesAcknowledged = 0;

    @Override
    public ProducerRecord<String, Employee> onSend(ProducerRecord<String, Employee> producerRecord) {
        messagesSent++;
        LOGGER.info("Intercepting message {} on send ", producerRecord.value().toString());

        /*
         Lets limit age to 100.
         This can also be done in constructor or setter. This is just to understand interceptor.
         */
        producerRecord.value().setAge(producerRecord.value().getAge()%100);

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        messagesAcknowledged++;
        LOGGER.info("Message is sent on topic {}, partition {} and offset {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
    }

    @Override
    public void close() {
        LOGGER.info("Total messages sent : {}. Total messages acknowledged {}", messagesSent, messagesAcknowledged);
        LOGGER.info("Closing employee producer interceptor");
    }

    @Override
    public void configure(Map<String, ?> map) {
        LOGGER.info("Starting employee producer interceptor");
    }
}
