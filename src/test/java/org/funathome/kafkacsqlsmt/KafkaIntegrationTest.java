package org.funathome.kafkacsqlsmt;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Integration test for debugging CSqlTransform with a real Kafka cluster.
 * 
 * INSTRUCTIONS:
 * 1. Update BOOTSTRAP_SERVERS to point to your Kafka cluster
 * 2. Update INPUT_TOPIC to the topic you want to consume from
 * 3. Configure any auth properties in getConsumerProperties() if needed
 * 4. Remove @Ignore annotation from the test you want to run
 * 5. Set breakpoints in CSqlTransform and run in debug mode
 * 
 * Run with: mvn test -Dtest=KafkaIntegrationTest#testConsumeAndTransform
 */
@Ignore("Integration test - requires real Kafka cluster. Remove @Ignore to run manually.")
public class KafkaIntegrationTest {

    // ============== CONFIGURE THESE ==============
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "test-topic";
    private static final String GROUP_ID = "test-consumer-group";
    
    // Transform configuration
    private static final String SQL_STATEMENT = "SELECT * FROM inputrecord ";
    private static final String DEFAULT_MESSAGE_JSON = null; // e.g., "{\"field1\": \"default\"}"
    private static final boolean SKIP_BYTES_ENABLED = true; // Enable to skip Schema Registry wire format bytes
    private static final int SKIP_BYTES = 5; // Confluent Schema Registry wire format uses 5 bytes
    // =============================================

    
    /**
     * Consume byte array messages (for skip-bytes testing with Schema Registry).
     */
    @Test
    public void testConsumeByteArrayAndTransform() {
        CSqlTransform<SinkRecord> transform = createTransformWithSkipBytes();
        
        Properties props = getConsumerProperties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            
            System.out.println("=== Consuming BYTE ARRAYS from topic: " + INPUT_TOPIC + " ===");
            System.out.println("=== Skip bytes enabled: " + SKIP_BYTES_ENABLED + ", bytes to skip: " + SKIP_BYTES + " ===");
            
            int messageCount = 0;
            int maxMessages = 10;
            
            while (messageCount < maxMessages) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                
                if (records.isEmpty()) {
                    System.out.println("No messages received, waiting...");
                    continue;
                }
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    messageCount++;
                    byte[] value = record.value();
                    byte[] key = record.key();
                    
                    System.out.println("\n=== Message " + messageCount + " ===");
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Key: " + (key != null ? new String(key, StandardCharsets.UTF_8) : "null"));
                    System.out.println("Value length: " + (value != null ? value.length : "null"));
                    
                    if (value != null) {
                        // Print first bytes as hex
                        StringBuilder hex = new StringBuilder("First 20 bytes (hex): ");
                        for (int i = 0; i < Math.min(20, value.length); i++) {
                            hex.append(String.format("%02X ", value[i]));
                        }
                        System.out.println(hex);
                        
                        // Print as string after skipping bytes
                        if (value.length > SKIP_BYTES) {
                            String jsonPart = new String(value, SKIP_BYTES, value.length - SKIP_BYTES, StandardCharsets.UTF_8);
                            System.out.println("Value after skipping " + SKIP_BYTES + " bytes: " + jsonPart);
                        }
                    }
                    
                    try {
                        // Convert key to string for SinkRecord
                        String keyString = key != null ? new String(key, StandardCharsets.UTF_8) : null;
                        
                        SinkRecord sinkRecord = new SinkRecord(
                                record.topic(),
                                record.partition(),
                                null,
                                keyString,
                                null,
                                value, // byte array
                                record.offset()
                        );
                        
                        // Apply the transform - SET BREAKPOINT HERE
                        SinkRecord transformed = transform.apply(sinkRecord);
                        
                        System.out.println("Transformed value: " + transformed.value());
                        
                    } catch (Exception e) {
                        System.err.println("Transform failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
        
        transform.close();
    }


    // ============== HELPER METHODS ==============

    private CSqlTransform<SinkRecord> createTransformWithSkipBytes() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, SQL_STATEMENT);
        configs.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, String.valueOf(SKIP_BYTES_ENABLED));
        configs.put(CSqlTransform.SKIP_BYTES_CONFIG, String.valueOf(SKIP_BYTES));
        if (DEFAULT_MESSAGE_JSON != null) {
            configs.put(CSqlTransform.DEFAULT_MESSAGE_CONFIG, DEFAULT_MESSAGE_JSON);
        }
        transform.configure(configs);
        return transform;
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // One at a time for debugging
        
        // === ADD AUTH CONFIG HERE IF NEEDED ===
        // For SASL/SCRAM:
        // props.put("security.protocol", "SASL_SSL");
        // props.put("sasl.mechanism", "SCRAM-SHA-512");
        // props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"<username>\" password=\"<password>\";");
        // props.put("ssl.endpoint.identification.algorithm", "");
        
        // For Azure Event Hubs:
        // props.put("security.protocol", "SASL_SSL");
        // props.put("sasl.mechanism", "PLAIN");
        // props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"<connection-string>\";");
        
        return props;
    }
}
