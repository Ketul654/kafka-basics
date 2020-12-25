package com.ketul.kafka.serde;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ketul.kafka.data.Employee;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Custom Employee deserializer
 */
public class EmployeeDeserializer implements Deserializer<Employee> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.info("Initializing Employee deserializer with {} configs", configs);
    }

    @Override
    public Employee deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Employee employee = null;
        try {
            employee = mapper.readValue(bytes, Employee.class);
        } catch (JsonParseException e) {
            LOGGER.error("Exception occurred while parsing employee Json : ", e);
        } catch (JsonMappingException e) {
            LOGGER.error("Exception occurred while mapping employee Json : ", e);
        } catch (IOException e) {
            LOGGER.error("IO Exception occurred while deserializing employee data : ", e);
        }
        return employee;
    }

    @Override
    public void close() {
        LOGGER.info("Closing Employee Deserializer");
    }
}
