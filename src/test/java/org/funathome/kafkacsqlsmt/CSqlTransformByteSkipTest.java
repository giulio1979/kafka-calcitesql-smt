package org.funathome.kafkacsqlsmt;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class CSqlTransformByteSkipTest {
    
    private final CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
    
    @After
    public void tearDown() {
        transform.close();
    }
    
    @Test
    public void testByteArrayWithSkipBytes() {
        // Simulate JSONSchemaConverter message: 1 magic byte + 4 schema ID bytes + JSON payload
        String jsonPayload = "{\"id\":123,\"name\":\"Test Product\"}";
        byte[] jsonBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
        
        // Create byte array with 5 prefix bytes (simulating schema registry format)
        byte[] messageBytes = new byte[5 + jsonBytes.length];
        messageBytes[0] = 0; // Magic byte
        messageBytes[1] = 0; // Schema ID byte 1
        messageBytes[2] = 0; // Schema ID byte 2
        messageBytes[3] = 0; // Schema ID byte 3
        messageBytes[4] = 1; // Schema ID byte 4 (schema ID = 1)
        System.arraycopy(jsonBytes, 0, messageBytes, 5, jsonBytes.length);
        
        Map<String, Object> config = new HashMap<>();
        config.put(CSqlTransform.STATEMENT_CONFIG, "select id, name from inputrecord");
        config.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, "true");
        config.put(CSqlTransform.SKIP_BYTES_CONFIG, "5");
        
        transform.configure(config);
        
        SinkRecord record = new SinkRecord("test", 0, null, null, null, messageBytes, 0);
        SinkRecord transformed = transform.apply(record);
        
        assertNotNull(transformed);
        assertNotNull(transformed.value());
        assertTrue(transformed.value() instanceof Struct);
        
        Struct result = (Struct) transformed.value();
        assertEquals(123, result.get("id"));
        assertEquals("Test Product", result.get("name"));
    }
    
    @Test
    public void testByteArrayWithCustomSkipBytes() {
        // Test with custom number of bytes to skip
        String jsonPayload = "{\"product\":\"Laptop\",\"price\":999.99}";
        byte[] jsonBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
        
        // Create byte array with 10 prefix bytes
        byte[] messageBytes = new byte[10 + jsonBytes.length];
        for (int i = 0; i < 10; i++) {
            messageBytes[i] = (byte) i;
        }
        System.arraycopy(jsonBytes, 0, messageBytes, 10, jsonBytes.length);
        
        Map<String, Object> config = new HashMap<>();
        config.put(CSqlTransform.STATEMENT_CONFIG, "select product, price from inputrecord");
        config.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, "true");
        config.put(CSqlTransform.SKIP_BYTES_CONFIG, "10");
        
        transform.configure(config);
        
        SinkRecord record = new SinkRecord("test", 0, null, null, null, messageBytes, 0);
        SinkRecord transformed = transform.apply(record);
        
        assertNotNull(transformed);
        assertNotNull(transformed.value());
        
        Struct result = (Struct) transformed.value();
        assertEquals("Laptop", result.get("product"));
        assertEquals(999.99, result.get("price"));
    }
    
    @Test
    public void testByteArrayWithoutSkipBytesEnabled() {
        // When skip bytes is not enabled, should treat byte array as regular UTF-8 JSON
        String jsonPayload = "{\"id\":456,\"status\":\"active\"}";
        byte[] messageBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
        
        Map<String, Object> config = new HashMap<>();
        config.put(CSqlTransform.STATEMENT_CONFIG, "select id, status from inputrecord");
        config.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, "false");
        
        transform.configure(config);
        
        SinkRecord record = new SinkRecord("test", 0, null, null, null, messageBytes, 0);
        SinkRecord transformed = transform.apply(record);
        
        assertNotNull(transformed);
        assertNotNull(transformed.value());
        
        Struct result = (Struct) transformed.value();
        assertEquals(456, result.get("id"));
        assertEquals("active", result.get("status"));
    }
    
    @Test(expected = org.apache.kafka.connect.errors.DataException.class)
    public void testByteArrayTooShortForSkip() {
        // Test error handling when byte array is shorter than skip bytes
        byte[] messageBytes = new byte[]{0, 1, 2}; // Only 3 bytes
        
        Map<String, Object> config = new HashMap<>();
        config.put(CSqlTransform.STATEMENT_CONFIG, "select * from inputrecord");
        config.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, "true");
        config.put(CSqlTransform.SKIP_BYTES_CONFIG, "5");
        
        transform.configure(config);
        
        SinkRecord record = new SinkRecord("test", 0, null, null, null, messageBytes, 0);
        transform.apply(record); // Should throw DataException
    }
    
    @Test
    public void testStringValueNotAffectedBySkipBytes() {
        // String values should not be affected by skip bytes configuration when disabled
        String jsonPayload = "{\"field\":\"value\"}";
        
        Map<String, Object> config = new HashMap<>();
        config.put(CSqlTransform.STATEMENT_CONFIG, "select field from inputrecord");
        config.put(CSqlTransform.SKIP_BYTES_ENABLED_CONFIG, "false"); // Explicitly disabled
        
        transform.configure(config);
        
        SinkRecord record = new SinkRecord("test", 0, null, null, null, jsonPayload, 0);
        SinkRecord transformed = transform.apply(record);
        
        assertNotNull(transformed);
        assertNotNull(transformed.value());
        
        Struct result = (Struct) transformed.value();
        assertEquals("value", result.get("field"));
    }
    
    
}
