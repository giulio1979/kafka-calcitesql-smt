package org.funathome.kafkacsqlsmt;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ByteSkipTransformTest {

    private ByteSkipTransform<SourceRecord> transform;

    @Before
    public void setUp() {
        transform = new ByteSkipTransform<>();
    }

    @Test
    public void testSkipBytesDefault() {
        // Configure with defaults (enabled=true, bytes=5)
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // Create a byte array with 5 bytes prefix + JSON
        String json = "{\"name\":\"test\",\"value\":123}";
        byte[] prefix = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01}; // 5 bytes prefix
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[prefix.length + jsonBytes.length];
        System.arraycopy(prefix, 0, combined, 0, prefix.length);
        System.arraycopy(jsonBytes, 0, combined, prefix.length, jsonBytes.length);

        SourceRecord record = createRecord(combined);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        assertTrue(result.value() instanceof Struct);
        Struct struct = (Struct) result.value();
        assertEquals("test", struct.get("name"));
        assertEquals(123, struct.get("value"));
    }

    @Test
    public void testSkipBytesCustomAmount() {
        // Configure with custom skip bytes
        Map<String, Object> config = new HashMap<>();
        config.put(ByteSkipTransform.SKIP_BYTES_CONFIG, "3");
        transform.configure(config);

        // Create a byte array with 3 bytes prefix + JSON
        String json = "{\"field\":\"data\"}";
        byte[] prefix = new byte[]{0x01, 0x02, 0x03}; // 3 bytes prefix
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[prefix.length + jsonBytes.length];
        System.arraycopy(prefix, 0, combined, 0, prefix.length);
        System.arraycopy(jsonBytes, 0, combined, prefix.length, jsonBytes.length);

        SourceRecord record = createRecord(combined);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("data", struct.get("field"));
    }

    @Test
    public void testSkipBytesDisabled() {
        // Configure with skip disabled
        Map<String, Object> config = new HashMap<>();
        config.put(ByteSkipTransform.SKIP_BYTES_ENABLED_CONFIG, "false");
        transform.configure(config);

        // Create a byte array with just JSON (no prefix)
        String json = "{\"message\":\"hello\"}";
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

        SourceRecord record = createRecord(jsonBytes);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("hello", struct.get("message"));
    }

    @Test(expected = DataException.class)
    public void testByteArrayTooShort() {
        Map<String, Object> config = new HashMap<>();
        config.put(ByteSkipTransform.SKIP_BYTES_CONFIG, "5");
        transform.configure(config);

        // Create a byte array that's too short
        byte[] shortArray = new byte[]{0x01, 0x02, 0x03}; // Only 3 bytes

        SourceRecord record = createRecord(shortArray);
        transform.apply(record);
    }

    @Test
    public void testStringInput() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        String json = "{\"key\":\"value\",\"number\":42}";
        SourceRecord record = createRecord(json);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("value", struct.get("key"));
        assertEquals(42, struct.get("number"));
    }

    @Test
    public void testMapInput() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("field1", "value1");
        inputMap.put("field2", 100);

        SourceRecord record = createRecord(inputMap);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("value1", struct.get("field1"));
        assertEquals(100, struct.get("field2"));
    }

    @Test
    public void testComplexNestedObject() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        // JSON with nested object
        String json = "{\"name\":\"test\",\"nested\":{\"inner\":\"value\"}}";
        byte[] prefix = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01};
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[prefix.length + jsonBytes.length];
        System.arraycopy(prefix, 0, combined, 0, prefix.length);
        System.arraycopy(jsonBytes, 0, combined, prefix.length, jsonBytes.length);

        SourceRecord record = createRecord(combined);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("test", struct.get("name"));
        // Nested objects should be serialized as JSON string
        String nestedValue = (String) struct.get("nested");
        assertTrue(nestedValue.contains("inner"));
        assertTrue(nestedValue.contains("value"));
    }

    @Test(expected = DataException.class)
    public void testNullValue() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        SourceRecord record = createRecord(null);
        transform.apply(record);
    }

    @Test
    public void testConfigDef() {
        assertNotNull(transform.config());
        assertTrue(transform.config().configKeys().containsKey(ByteSkipTransform.SKIP_BYTES_CONFIG));
        assertTrue(transform.config().configKeys().containsKey(ByteSkipTransform.SKIP_BYTES_ENABLED_CONFIG));
    }

    @Test
    public void testDifferentDataTypes() {
        Map<String, Object> config = new HashMap<>();
        transform.configure(config);

        String json = "{\"stringField\":\"text\",\"intField\":42,\"longField\":9999999999,\"doubleField\":3.14,\"boolField\":true}";
        byte[] prefix = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01};
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[prefix.length + jsonBytes.length];
        System.arraycopy(prefix, 0, combined, 0, prefix.length);
        System.arraycopy(jsonBytes, 0, combined, prefix.length, jsonBytes.length);

        SourceRecord record = createRecord(combined);
        SourceRecord result = transform.apply(record);

        assertNotNull(result);
        Struct struct = (Struct) result.value();
        assertEquals("text", struct.get("stringField"));
        assertEquals(42, struct.get("intField"));
        assertEquals(9999999999L, struct.get("longField"));
        assertEquals(3.14, struct.get("doubleField"));
        assertEquals(true, struct.get("boolField"));
    }

    private SourceRecord createRecord(Object value) {
        return new SourceRecord(
                null, // sourcePartition
                null, // sourceOffset
                "test-topic",
                0, // partition
                null, // keySchema
                null, // key
                null, // valueSchema
                value,
                System.currentTimeMillis()
        );
    }
}
