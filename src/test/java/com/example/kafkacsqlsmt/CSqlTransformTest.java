package com.example.kafkacsqlsmt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;

public class CSqlTransformTest {
    @Test
    public void testStringInput() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":\"foo\",\"b\":\"bar\"}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals("foo", struct.get("a"));
        assertEquals("bar", struct.get("b"));
    }

    // Avro and JSON Schema tests are stubs, as full Avro/JSON Schema support would require additional libraries and setup
    @Test
    public void testAvroInput() {
        // TODO: Implement Avro record test if Avro libraries are available
    }

    @Test
    public void testJsonSchemaInput() {
        // TODO: Implement JSON Schema record test if JSON Schema libraries are available
    }

    @Test
    public void testJsonObjectField() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select obj from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"obj\":{\"x\":1,\"y\":2}}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNotNull(struct.get("obj"));
    }

    @Test
    public void testNestedArrayField() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select arr from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"arr\":[[1,2],[3,4]]}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNotNull(struct.get("arr"));
    }
    @Test
    public void testSchemalessJsonInput() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":1,\"b\":2,\"c\":\"{\\\"json\\\":5}\",\"arr\":[1,2,3]}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
    assertNotNull(output.valueSchema());
    assertTrue(output.value() instanceof Struct);
    Struct struct = (Struct) output.value();
    assertEquals(1, struct.get("a"));
    assertEquals(2, struct.get("b"));
    }

    @Test
    public void testStructInput() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);
        Schema schema = SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).field("b", Schema.INT32_SCHEMA).build();
        Struct struct = new Struct(schema).put("a", 1).put("b", 2);
        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        assertNotNull(output.valueSchema());
        assertTrue(output.value() instanceof Struct);
        Struct outStruct = (Struct) output.value();
        assertEquals(1, outStruct.get("a"));
        assertEquals(2, outStruct.get("b"));
    }

    @Test
    public void testNestedJsonField() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, c from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":1,\"c\":\"{\\\"json\\\":5}\"}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(1, struct.get("a"));
        assertNotNull(struct.get("c"));
    }

    @Test
    public void testArrayField() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select arr from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":1,\"arr\":[1,2,3]}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNotNull(struct.get("arr"));
    }

    @Test
    public void testMissingFieldInSQL() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, x from inputrecord");
        transform.configure(configs);
        Schema schema = SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).field("b", Schema.INT32_SCHEMA).build();
        Struct struct = new Struct(schema).put("a", 1).put("b", 2);
        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct outStruct = (Struct) output.value();
        assertEquals(1, outStruct.get("a"));
        assertNull(outStruct.get("x"));
    }

    @Test
    public void testTypeMismatch() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":\"string\",\"b\":2}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals("string", struct.get("a"));
        assertEquals(2, struct.get("b"));
    }

    @Test
    public void testNullValues() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);
        String inputJson = "{\"a\":null,\"b\":2}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNull(struct.get("a"));
        assertEquals(2, struct.get("b"));
    }
}
