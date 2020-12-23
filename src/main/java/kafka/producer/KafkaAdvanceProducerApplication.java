package kafka.producer;

import kafka.metrics.ProducerMetricsReporter;
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
 * This has advance configurations for acks, batching, retry, compression etc.
 * We can play around these configurations and check metrics logs to understand the impact.
 */
public class KafkaAdvanceProducerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdvanceProducerApplication.class);
    public static void main(String[] args) {

        Properties properties = new Properties();
        // Mandatory properties for producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
         Batching and compression configs
         Enable batching and try different linger time, batch size and compression. Observer metrics.
         Disable batching and compression and observe metrics. You can disable batching by setting BATCH_SIZE_CONFIG to 0.
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, KafkaConstants.LINGER_MS);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConstants.BATCH_SIZE);
        //properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConstants.SNAPPY_COMPRESSION_TYPE);

        /*
         Change min.insync.replicas to 3 for all 3 brokers in broker configuration files and bounce them all.
         Make acks to all.
         Start this producer.
         Kill one broker while this producer is running.
         Describe topic and observer changes in isr.
         You should get exception for not having enough replicas.
         Start replication verification on terminal and observe what happens.
         Make the ACKS_CONFIG 1 and 0 and see what happens.
         */
        properties.put(ProducerConfig.ACKS_CONFIG, KafkaConstants.ALL_BROKER_ACKS);

        /*
            Retry mechanism
            Change min.insync.replicas to 2 for all 3 brokers in broker configuration files and bounce them all.
            Start producing messages. Bring down 2 brokers when producer is producing messages and observe metrics and logs.
            Bring up brokers again and observe metrics and logs.
         */
        properties.put(ProducerConfig.RETRIES_CONFIG, KafkaConstants.RETRIES);
        // This can make sure messages are in correct order if set to 1
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConstants.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, KafkaConstants.REQUEST_TIMEOUT_MS);
        // Try retry only after sometime
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, KafkaConstants.RETRY_BACKOFF_MS);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);

        // Stating metrics reporter
        ProducerMetricsReporter reporter = new ProducerMetricsReporter(kafkaProducer);
        Thread thread = new Thread(reporter);
        thread.start();

        try {
            for (int i = 0; i < 10000; i++) {
                ProducerRecord record = new ProducerRecord(KafkaConstants.MULTI_PARTITION_TOPIC_NAME, String.format("Compressed Message :%s", UUID.randomUUID().toString()));
                kafkaProducer.send(record);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception occurred while producing message : ", ex);
        } finally {
            kafkaProducer.close();
            reporter.stop();
        }
    }
}
