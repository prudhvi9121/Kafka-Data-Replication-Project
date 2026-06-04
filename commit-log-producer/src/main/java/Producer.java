import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

class EventValue {
    @JsonProperty("status")
    private final String status;

    public EventValue(String status) { this.status = status; }
    public String getStatus() { return status; }
}

class CommitLogEvent {
    @JsonProperty("event_id")  private final String eventId;
    @JsonProperty("timestamp") private final long timestamp;
    @JsonProperty("op_type")   private final String opType;
    @JsonProperty("key")       private final String key;
    @JsonProperty("value")     private final EventValue value;

    public CommitLogEvent(String key, String status) {
        this.eventId   = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis() / 1000L;
        this.opType    = "UPDATE";
        this.key       = key;
        this.value     = new EventValue(status);
    }

    public String getEventId()   { return eventId; }
    public long getTimestamp()   { return timestamp; }
    public String getOpType()    { return opType; }
    public String getKey()       { return key; }
    public EventValue getValue() { return value; }
}

public class Producer {

    private static final String TOPIC   = "commit-log";
    private static final String DOC_KEY = "doc:8f7b";

    public static void main(String[] args) {
        int messageCount = 1000;
        for (int i = 0; i < args.length; i++) {
            if ("--count".equals(args[i]) && i + 1 < args.length) {
                try {
                    messageCount = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid value for --count. Using default: " + messageCount);
                }
            }
        }
        runProducer(messageCount);
    }

    private static void runProducer(int messageCount) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        System.out.println("Initializing Production-Grade Producer Client...");
        System.out.println("Connecting to Target Cluster: " + bootstrapServers);
        System.out.println("Target message generation volume: " + messageCount);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,    "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper mapper = new ObjectMapper();

        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= messageCount; i++) {
                CommitLogEvent event = new CommitLogEvent(DOC_KEY, "archived");
                String json = mapper.writeValueAsString(event);
                producer.send(new ProducerRecord<>(TOPIC, event.getKey(), json), (meta, err) -> {
                    if (err != null) System.err.println("CRITICAL DELIVERY FAILURE: " + err.getMessage());
                });
            }
            producer.flush();
            System.out.println("SUCCESS: Successfully streamed " + messageCount + " JSON events into topic '" + TOPIC + "'!");
        } catch (Exception e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace();
        }
    }
}