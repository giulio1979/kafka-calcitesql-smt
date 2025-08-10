package org.funathome.kafkacsqlsmt;

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
    public void testJoinWithStringifiedJsonArraySubtable() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a.*, b.*\n  from inputrecord a\n  join \"inputrecord.lineitems\" b on true");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"invoicenumber\": 2,\n"
                + "  \"lineitems\": \"[ {\\\"product\\\": \\\"tomatoes\\\", \\\"price\\\": \\\"3\\\" } ]\"\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(2, struct.get("invoicenumber"));
        String lineitemsJson = (String) struct.get("lineitems");
        assertTrue(lineitemsJson.replaceAll("\\s", "").contains("{\"product\":\"tomatoes\",\"price\":\"3\"}"));
        assertEquals("tomatoes", struct.get("product"));
        assertEquals("3", struct.get("price"));
    }

    @Test
    public void testJoinWithNestedArray() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a.*, b.*\n"
                        + "  from inputrecord a\n"
                        + "  join \"inputrecord.lineitems\" b on true");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"invoicenumber\": 2,\n"
                + "  \"lineitems\": [\n"
                + "    {\n"
                + "      \"product\": \"tomatoes\",\n"
                + "      \"price\": \"3\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(2, struct.get("invoicenumber"));
        assertEquals("tomatoes", struct.get("product"));
        assertEquals("3", struct.get("price"));
        Object lineitems = struct.get("lineitems");
        assertNotNull(lineitems);
        assertTrue(lineitems.toString().contains("tomatoes"));
    }

    @Test
    public void testCustomBodyWithPricesArray() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select\n"
                        + "  a.invoicenumber,\n"
                        + "  b.product,\n"
                        + "  ARRAY[ROW(b.price, 'usd')] as prices\n"
                        + "from inputrecord a\n"
                        + "  join \"inputrecord.lineitems\" b on true");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"invoicenumber\": 2,\n"
                + "  \"lineitems\": [\n"
                + "    {\n"
                + "      \"product\": \"tomatoes\",\n"
                + "      \"price\": \"3\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(2, struct.get("invoicenumber"));
        assertEquals("tomatoes", struct.get("product"));
        Object pricesObj = struct.get("prices");
        assertNotNull(pricesObj);
        // Check array structure and contents
        String pricesStr = pricesObj.toString();
        assertTrue(pricesStr.contains("3"));
        assertTrue(pricesStr.contains("usd"));
    }

    @Test
    public void testStringInput() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, b\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": \"foo\",\n"
                + "  \"b\": \"bar\"\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals("foo", struct.get("a"));
        assertEquals("bar", struct.get("b"));
    }

    @Test
    public void testSelectStarFromInputRecord() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select *\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"id\": 1,\n"
                + "  \"name\": \"Alice\",\n"
                + "  \"active\": true\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(1, struct.get("id"));
        assertEquals("Alice", struct.get("name"));
        assertEquals(true, struct.get("active"));
    }

    @Test
    public void testJsonObjectField() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select obj\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"obj\": {\n"
                + "    \"x\": 1,\n"
                + "    \"y\": 2\n"
                + "  }\n"
                + "}";
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select arr\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"arr\": [\n"
                + "    [1, 2],\n"
                + "    [3, 4]\n"
                + "  ]\n"
                + "}";
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, b\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": 1,\n"
                + "  \"b\": 2,\n"
                + "  \"c\": \"{\\\"json\\\":5}\",\n"
                + "  \"arr\": [1, 2, 3]\n"
                + "}";
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, b\n"
                        + "  from inputrecord");
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, c\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": 1,\n"
                + "  \"c\": \"{\\\"json\\\":5}\"\n"
                + "}";
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select arr\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": 1,\n"
                + "  \"arr\": [1, 2, 3]\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNotNull(struct.get("arr"));
    }

    @Test
    public void testTypeMismatch() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, b\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": \"string\",\n"
                + "  \"b\": 2\n"
                + "}";
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
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select a, b\n"
                        + "  from inputrecord");
        transform.configure(configs);
        String inputJson = "{\n"
                + "  \"a\": null,\n"
                + "  \"b\": 2\n"
                + "}";
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertNull(struct.get("a"));
        assertEquals(2, struct.get("b"));
    }
}
