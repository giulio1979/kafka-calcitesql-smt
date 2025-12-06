package org.funathome.kafkacsqlsmt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigDef;

/**
 * A Kafka Connect SMT (Single Message Transform) that removes a configurable number of bytes
 * from the beginning of byte array messages and parses the remaining content as JSON.
 * 
 * This is useful for handling messages from systems like Schema Registry that prepend
 * metadata bytes (e.g., magic byte + schema ID) before the actual JSON payload.
 * 
 * Configuration:
 * - kafka.connect.transform.byteskip.enabled: Enable/disable byte skipping (default: true)
 * - kafka.connect.transform.byteskip.bytes: Number of bytes to skip (default: 5)
 */
public class ByteSkipTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ByteSkipTransform.class);
    
    public static final String SKIP_BYTES_CONFIG = "kafka.connect.transform.byteskip.bytes";
    public static final String SKIP_BYTES_ENABLED_CONFIG = "kafka.connect.transform.byteskip.enabled";
    
    private static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_TYPE = 
            new TypeReference<Map<String, Object>>() {};
    
    private int skipBytes = 5; // Default: 5 bytes for JSONSchemaConverter (1 magic byte + 4 schema ID)
    private boolean skipBytesEnabled = true;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs) {
        this.skipBytesEnabled = configs.containsKey(SKIP_BYTES_ENABLED_CONFIG) ? 
            Boolean.parseBoolean(configs.get(SKIP_BYTES_ENABLED_CONFIG).toString()) : true;
        
        if (configs.containsKey(SKIP_BYTES_CONFIG)) {
            this.skipBytes = Integer.parseInt(configs.get(SKIP_BYTES_CONFIG).toString());
        }
        
        log.info("ByteSkipTransform configured: skipBytesEnabled={}, skipBytes={}", 
                skipBytesEnabled, skipBytes);
    }

    @Override
    public R apply(R record) {
        try {
            Object value = record.value();
            
            if (value == null) {
                throw new DataException("Record value is null; cannot apply byte skip transformation");
            }
            
            log.debug("ByteSkipTransform received value type: {}", value.getClass().getName());
            
            Map<String, Object> jsonMap;
            
            if (value instanceof byte[]) {
                byte[] bytes = (byte[]) value;
                
                if (skipBytesEnabled) {
                    log.info("Processing byte array with skip bytes enabled. Array length: {}, bytes to skip: {}", 
                            bytes.length, skipBytes);
                    
                    if (bytes.length <= skipBytes) {
                        throw new DataException("Byte array too short to skip " + skipBytes + 
                                " bytes. Length: " + bytes.length);
                    }
                    
                    // Skip the first N bytes (schema registry magic byte + schema ID)
                    String jsonString = new String(bytes, skipBytes, bytes.length - skipBytes, StandardCharsets.UTF_8);
                    log.debug("Skipped {} bytes from byte array, parsing remaining as JSON: {}", skipBytes, jsonString);
                    jsonMap = readJsonMap(jsonString);
                } else {
                    // No skip - treat entire byte array as UTF-8 JSON string
                    String jsonString = new String(bytes, StandardCharsets.UTF_8);
                    jsonMap = readJsonMap(jsonString);
                }
            } else if (value instanceof String) {
                // Pass through strings as JSON
                jsonMap = readJsonMap((String) value);
            } else if (value instanceof Map) {
                // Already a map, just sanitize it
                jsonMap = sanitizeMap((Map<?, ?>) value);
            } else if (value instanceof Struct) {
                // Convert Struct to Map
                jsonMap = structToMap((Struct) value);
            } else {
                throw new DataException("Unsupported record value type: " + value.getClass().getName());
            }
            
            log.debug("ByteSkipTransform parsed JSON map with {} fields: {}", jsonMap.size(), jsonMap.keySet());
            
            // Build output schema and struct from the parsed JSON
            Schema outputSchema = buildSchemaFromMap(jsonMap);
            Struct outputStruct = buildStructFromMap(jsonMap, outputSchema);
            
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    outputSchema,
                    outputStruct,
                    record.timestamp());
                    
        } catch (Exception e) {
            log.error("ByteSkipTransform error: {}", e.getMessage(), e);
            throw new DataException("Failed to apply ByteSkipTransform", e);
        }
    }

    private Map<String, Object> readJsonMap(String json) throws IOException {
        return objectMapper.readValue(json, MAP_STRING_OBJECT_TYPE);
    }

    private Map<String, Object> sanitizeMap(Map<?, ?> source) {
        Map<String, Object> sanitized = new HashMap<>();
        if (source == null) {
            return sanitized;
        }
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            String key = entry.getKey() == null ? null : entry.getKey().toString();
            sanitized.put(key, entry.getValue());
        }
        return sanitized;
    }

    private Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            map.put(field.name(), struct.get(field));
        }
        return map;
    }

    /**
     * Build a Kafka Connect Schema from a JSON map.
     * Complex objects (maps/lists) are serialized as strings.
     */
    private Schema buildSchemaFromMap(Map<String, Object> map) {
        SchemaBuilder builder = SchemaBuilder.struct();
        
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            Schema fieldSchema = inferSchemaFromValue(value);
            builder.field(fieldName, fieldSchema);
        }
        
        return builder.build();
    }

    /**
     * Infer a Kafka Connect Schema from a Java value.
     */
    private Schema inferSchemaFromValue(Object value) {
        if (value == null) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof String) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.OPTIONAL_FLOAT32_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        } else if (value instanceof Map || value instanceof java.util.List) {
            // Complex types are serialized as JSON strings
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    /**
     * Build a Kafka Connect Struct from a JSON map using the given schema.
     */
    private Struct buildStructFromMap(Map<String, Object> map, Schema schema) {
        Struct struct = new Struct(schema);
        
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object value = map.get(field.name());
            
            // Serialize complex types as JSON strings
            if (value instanceof Map || value instanceof java.util.List) {
                try {
                    value = objectMapper.writeValueAsString(value);
                } catch (Exception e) {
                    log.warn("Failed to serialize complex value for field '{}': {}", field.name(), e.getMessage());
                    value = value.toString();
                }
            }
            
            struct.put(field.name(), value);
        }
        
        return struct;
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(SKIP_BYTES_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, 
                        "Enable skipping bytes at the beginning of byte array messages (default: true)")
                .define(SKIP_BYTES_CONFIG, ConfigDef.Type.INT, 5, ConfigDef.Importance.LOW, 
                        "Number of bytes to skip (default: 5 for Schema Registry magic byte + schema ID)");
    }
}
