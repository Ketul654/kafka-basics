package kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * This will be involved while rebalance happens
 * Trigger rebalane in consumer group by killing a consumer and see what happens
 */
public class CustomRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomRebalanceListener.class);
    private final KafkaConsumer<String, String> kafkaConsumer;
    public CustomRebalanceListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        StringBuilder builder = new StringBuilder("Partitions revoked : ");
        collection.stream().forEach(topicPartition -> builder.append(String.format("[%s]",topicPartition.toString())));
        LOGGER.info(builder.toString());
        // Committing offsets before losing partition ownership
        kafkaConsumer.commitSync();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        StringBuilder builder = new StringBuilder("Partitions assigned : ");
        collection.stream().forEach(topicPartition -> builder.append(String.format("[%s]",topicPartition.toString())));
        LOGGER.info(builder.toString());
    }
}
