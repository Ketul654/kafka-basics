package kafka.metrics;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProducerMetricsReporter implements Runnable{
    private final Logger logger = LoggerFactory.getLogger(ProducerMetricsReporter.class);
    private Producer<String, String> producer;
    private boolean isRunning = true;
    private final Set<String> metricFilterNames = new HashSet<>();
    public ProducerMetricsReporter(final Producer<String, String> producer) {
        this.producer = producer;
        initializeMetricNameFilter();
    }

    private void initializeMetricNameFilter() {
        metricFilterNames.add("record-queue-time-avg");
        metricFilterNames.add("record-send-rate");
        metricFilterNames.add("records-per-request-avg");
        metricFilterNames.add("request-size-max");
        metricFilterNames.add("network-io-rate");
        metricFilterNames.add("batch-size-avg");
        metricFilterNames.add("response-rate");
        metricFilterNames.add("requests-in-flight");
        metricFilterNames.add("incoming-byte-rate");
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
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Metrics thread has been interrupted : ", e);
            }
        }
        logger.info("Stopping metrics reporter");
    }

    private void printMetrics(Map<MetricName,? extends Metric> metrics) {
        StringBuilder builder = new StringBuilder();
        metrics.entrySet().stream()
                .filter(metricNameEntry -> metricFilterNames.contains(metricNameEntry.getKey().name()))
                .forEach(metric ->
                        builder.append(String.format("[%s,%s,%s]", metric.getKey().name(), metric.getKey().description(), metric.getValue().metricValue())));

        logger.info(builder.toString());
    }
}
