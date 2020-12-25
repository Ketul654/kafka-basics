package com.ketul.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

/**
 * Custom partitioner to decide partition to send message to
 */
public class KeyValueHashPartitioner implements Partitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueHashPartitioner.class);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numberOfPartitions = partitions.size();

        if(keyBytes==null || !(key instanceof String) || valueBytes==null || !(value instanceof String))
            throw new InvalidRecordException("Key and Value cannot be null");
        else {
            String strKey = (String)key;
            String strVal = (String)value;
            int keyHash = strKey.hashCode();
            int valHash = strVal.hashCode();
            int maxHash = Math.max(keyHash, valHash);
            return maxHash%numberOfPartitions;
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing custom partitioner");
    }

    @Override
    public void configure(Map<String, ?> map) {
        LOGGER.info("Initializing custom partitioner");
    }
}
