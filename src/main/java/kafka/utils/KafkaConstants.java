package kafka.utils;

public class KafkaConstants {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
    public static final String SINGLE_PARTITION_TOPIC_NAME = "single-partition-topic";
    public static final String MULTI_PARTITION_TOPIC_NAME = "multi-partition-topic";
    public static final String EMPLOYEE_TOPIC_NAME = "employee";
    public static final String COMPRESSION_TYPE = "gzip";
    public static final String CLIENT_DNS_LOOKUP = "resolve_canonical_bootstrap_servers_only";
    public static final String CLIENT_ID = "ketul-producer";
    public static final String TRANSACTIONAL_ID = "tran-1";

    public static final int RETRIES = 3;
    public static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5;

    public static final long RETRY_BACKOFF_MS = 1000;
    public static final long DELIVERY_TIMEOUT_MS = 40000;
    public static final long REQUEST_TIMEOUT_MS = 20000;
    public static final long LINGER_MS = 10000;
    public static final long BATCH_SIZE = 32000;
    public static final long CONNECTIONS_MAX_IDLE_MS = 10000;
    public static final long MAX_BLOCK_MS = 120000;
    public static final long MAX_REQUEST_SIZE = 10000;

    public static final long METADATA_MAX_IDLE = 600000;
    public static final long METADATA_MAX_AGE_MS = 120000;
    public static final long RECONNECT_BACKOFF_MS = 200;
    public static final long TRANSACTION_TIMEOUT = 30000;

    public static final boolean ENABLE_IDEMPOTENCE = true;

    private KafkaConstants(){
    }
}
