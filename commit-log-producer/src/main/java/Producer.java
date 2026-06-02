import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

// 1. TOP-LEVEL SCHEMAS (No longer nested inside anything)
class EventValue {
    @JsonProperty("status")
    private final String status;

    public EventValue(String status) {
        this.status = status;
    }

    public String getStatus() { return status; }
}

class CommitLogEvent {
    @JsonProperty("event_id")
    private final String eventId;

    @JsonProperty("timestamp")
    private final long timestamp;

    @JsonProperty("op_type")
    private final String opType;

    @JsonProperty("key")
    private final String key;

    @JsonProperty("value")
    private final EventValue value;

    public CommitLogEvent(String key, String status) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis() / 1000L;
        this.opType = "UPDATE";
        this.key = key;
        this.value = new EventValue(status);
    }

    public String getEventId() { return eventId; }
    public long getTimestamp() { return timestamp; }
    public String getOpType() { return opType; }
    public String getKey() { return key; }
    public EventValue getValue() { return value; }
}

// 2. THE MAIN WRITER CLASS
public class Producer {

    public static void main(String[] args) {
        int messageCount = 1000; // Default fallback count

        // Parse arguments for --count N
        for (int i = 0; i < args.length; i++) {
            if ("--count".equals(args[i]) && i + 1 < args.length) {
                try {
                    messageCount = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid entry for --count. Using default: " + messageCount);
                }
            }
        }

        // Run the engine
        runProducer(messageCount);
    }

    private static void runProducer(int messageCount) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        System.out.println("Initializing Production-Grade Producer Client...");
        System.out.println("Connecting to Target Cluster: " + bootstrapServers);
        System.out.println("Target message generation volume: " + messageCount);

        Properties configProps = new Properties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); 
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper jsonMapper = new ObjectMapper();

        try (org.apache.kafka.clients.producer.Producer<String, String> kafkaProducer = new KafkaProducer<>(configProps)) {
            String topicName = "commit-log";

            for (int index = 1; index <= messageCount; index++) {
                String docKey = "doc:8f7b";
                CommitLogEvent generatedEvent = new CommitLogEvent(docKey, "archived");

                String jsonPayload = jsonMapper.writeValueAsString(generatedEvent);
                ProducerRecord<String, String> recordEnvelope = new ProducerRecord<>(topicName, generatedEvent.getKey(), jsonPayload);

                kafkaProducer.send(recordEnvelope, (metadata, pipelineError) -> {
                    if (pipelineError != null) {
                        System.err.println("CRITICAL DELIVERY FAILURE: " + pipelineError.getMessage());
                    }
                });
            }

            kafkaProducer.flush();
            System.out.println("SUCCESS: Successfully streamed " + messageCount + " JSON events into topic '" + topicName + "'!");

        } catch (Exception fatalException) {
            System.err.println("FATAL RUNTIME STOPPAGE: " + fatalException.getMessage());
            fatalException.printStackTrace();
        }
    }
}