package com.ketul.kafka.metrics;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This prints producer metrics which can be used to optimize producer
 */
public class ProducerMetricsReporter implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerMetricsReporter.class);
    private final Producer<String, String> producer;
    private final Set<String> metricNamesFilter = new HashSet<>();
    private boolean isRunning = true;

    public ProducerMetricsReporter(final Producer<String, String> producer) {
        this.producer = producer;
        initializeMetricNameFilter();
    }

    private void initializeMetricNameFilter() {
        metricNamesFilter.add("record-queue-time-avg");
        metricNamesFilter.add("record-send-rate");
        metricNamesFilter.add("records-per-request-avg");
        metricNamesFilter.add("request-size-max");
        metricNamesFilter.add("network-io-rate");
        metricNamesFilter.add("batch-size-avg");
        metricNamesFilter.add("response-rate");
        metricNamesFilter.add("requests-in-flight");
        metricNamesFilter.add("incoming-byte-rate");
        metricNamesFilter.add("compression-rate-avg");
    }

    public void stop(){
        isRunning = false;
    }
    
    @Override
    public void run() {
        while (isRunning) {
            try {
                final Map<MetricName, ? extends Metric> metrics = producer.metrics();
                printMetrics(metrics);
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOGGER.error("Metrics thread has been interrupted : ", e);
            }
        }
        LOGGER.info("Stopping metrics reporter");
    }

    private void printMetrics(Map<MetricName,? extends Metric> metrics) {
        StringBuilder builder = new StringBuilder("\n");
        metrics.entrySet().stream()
                .filter(metricNameEntry -> metricNamesFilter.contains(metricNameEntry.getKey().name()))
                .forEach(metric ->
                        builder.append(String.format("[%s,%s,%s]\n", metric.getKey().name(), metric.getKey().description(), metric.getValue().metricValue())));

        LOGGER.info(builder.toString());
    }
}
