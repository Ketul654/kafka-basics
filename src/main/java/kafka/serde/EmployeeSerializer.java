package kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Employee;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EmployeeSerializer implements Serializer<Employee> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.info("Initializing Employee Serializer with {} configs", configs);
    }

    @Override
    public byte[] serialize(String s, Employee employee) {
        ObjectMapper mapper = new ObjectMapper();
        byte[] serializedData = null;
        try {
            serializedData = mapper.writeValueAsString(employee).getBytes();
        } catch (JsonProcessingException e) {
            LOGGER.error("Error occurred while serialising employee data : ", e);
        }
        return serializedData;
    }

    @Override
    public void close() {
        LOGGER.info("Closing Employee Serializer");
    }
}
