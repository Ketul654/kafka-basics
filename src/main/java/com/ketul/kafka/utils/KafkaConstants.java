package com.ketul.kafka.utils;

public class KafkaConstants {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static final String SINGLE_PARTITION_TOPIC_NAME = "single-partition-topic";
    public static final String MULTI_PARTITION_TOPIC_NAME = "multi-partition-topic";
    public static final String EMPLOYEE_TOPIC_NAME = "employee";
    public static final String GZIP_COMPRESSION_TYPE = "gzip";
    public static final String SNAPPY_COMPRESSION_TYPE = "snappy";
    public static final String CLIENT_DNS_LOOKUP = "resolve_canonical_bootstrap_servers_only";
    public static final String CLIENT_ID = "ketul's-producer";
    public static final String TRANSACTIONAL_ID = "transaction-1";
    public static final String ALL_BROKER_ACKS = "all";
    public static final String ONLY_LEADER_ACKS = "1";
    public static final String NO_ACKS = "0";

    public static final String CONSUMER_GROUP_1_ID = "consumer-group-1";

    public static final String ISOLATION_LEVEL_COMMITTED = "read_committed";
    public static final String ISOLATION_LEVEL_UNCOMMITTED = "read_uncommitted";

    public static final int RETRIES = 3;
    public static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5;
    public static final int RETRY_BACKOFF_MS = 1000;
    public static final int DELIVERY_TIMEOUT_MS = 40000;
    public static final int REQUEST_TIMEOUT_MS = 10000;
    public static final int LINGER_MS = 5 * 1000;
    public static final int BATCH_SIZE = 1024 * 256;
    public static final int CONNECTIONS_MAX_IDLE_MS = 10000;
    public static final int MAX_BLOCK_MS = 120000;
    public static final int MAX_REQUEST_SIZE = 10000;
    public static final int METADATA_MAX_IDLE = 600000;
    public static final int METADATA_MAX_AGE_MS = 120000;
    public static final int RECONNECT_BACKOFF_MS = 200;
    public static final int RECONNECT_BACKOFF_MAX_MS = 10000;
    public static final int TRANSACTION_TIMEOUT = 30000;
    public static final int FETCH_MIN_BYTES = 5 * 1024;
    public static final int FETCH_MAX_BYTES = 10 * 1024;
    public static final int FETCH_MAX_WAIT_MS = 10000;
    public static final int MAX_PARTITION_FETCH_BYTES = 20 * 1024;
    public static final int MAX_POLL_RECORDS = 2000;
    public static final int MAX_POLL_INTERVAL_MS = 100;

    public static final boolean ENABLE_IDEMPOTENCE = true;

    private KafkaConstants(){
    }
}
