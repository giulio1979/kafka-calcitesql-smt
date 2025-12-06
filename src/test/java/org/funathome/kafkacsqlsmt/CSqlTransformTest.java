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
        // With flattening, nested objects become prefixed fields: obj.x -> obj_x, obj.y -> obj_y
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select obj_x, obj_y\n"
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
        assertEquals(1, struct.get("obj_x"));
        assertEquals(2, struct.get("obj_y"));
    }

    @Test
    public void testNestedArrayField() {
        // With flattening, arrays become JSON strings
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
        // Array is serialized to JSON string during flattening
        assertEquals("[[1,2],[3,4]]", struct.get("arr"));
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

    @Test
    public void testDefaultMessageDeepMerge() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        
        // Configure a default message with default values for missing fields
        String defaultMessage = "{\n"
                + "  \"id\": 0,\n"
                + "  \"name\": \"Unknown\",\n"
                + "  \"CustomerMaterialNumber\": \"N/A\",\n"
                + "  \"quantity\": 1\n"
                + "}";
        
        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select id, name, CustomerMaterialNumber, quantity\n"
                        + "  from inputrecord");
        configs.put(CSqlTransform.DEFAULT_MESSAGE_CONFIG, defaultMessage);
        transform.configure(configs);
        
        // Input JSON is missing CustomerMaterialNumber and quantity fields
        String inputJson = "{\n"
                + "  \"id\": 123,\n"
                + "  \"name\": \"Test Product\"\n"
                + "}";
        
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        // Values from input take precedence
        assertEquals(123, struct.get("id"));
        assertEquals("Test Product", struct.get("name"));
        // Missing fields get default values from the default message
        assertEquals("N/A", struct.get("CustomerMaterialNumber"));
        assertEquals(1, struct.get("quantity"));
    }

        @Test
        public void testConsecutiveRecordsDoNotLeakState() {
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                                "select id, name, status\n"
                                                + "  from inputrecord");
                String defaultMessage = "{"
                                + "\"id\": 0, \"name\": \"Unknown\", \"status\": \"NEW\""
                                + "}";
                configs.put(CSqlTransform.DEFAULT_MESSAGE_CONFIG, defaultMessage);
                transform.configure(configs);

                String firstRecordJson = "{\"id\": 101, \"name\": \"Alpha\", \"status\": \"FIRST\"}";
                SinkRecord record1 = new SinkRecord("topic", 0, null, null, null, firstRecordJson, 0);
                SinkRecord output1 = transform.apply(record1);
                assertNotNull(output1);
                Struct struct1 = (Struct) output1.value();
                assertEquals(101, struct1.get("id"));
                assertEquals("Alpha", struct1.get("name"));
                assertEquals("FIRST", struct1.get("status"));

                String secondRecordJson = "{\"id\": 202}";
                SinkRecord record2 = new SinkRecord("topic", 0, null, null, null, secondRecordJson, 0);
                SinkRecord output2 = transform.apply(record2);
                assertNotNull(output2);
                Struct struct2 = (Struct) output2.value();
                assertEquals(202, struct2.get("id"));
                assertEquals("Unknown", struct2.get("name"));
                assertEquals("NEW", struct2.get("status"));
        }

        @Test
        public void testFlattenMapsDisabled() {
                // When flatten.maps is false, nested objects become JSON strings
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                        "select obj from inputrecord");
                configs.put(CSqlTransform.FLATTEN_MAPS_CONFIG, "false");
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
                // Nested object should be serialized as JSON string
                assertEquals("{\"x\":1,\"y\":2}", struct.get("obj"));
        }

        @Test
        public void testFlattenMapsEnabledExplicitly() {
                // When flatten.maps is explicitly true, nested objects become flattened fields
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                        "select obj_x, obj_y from inputrecord");
                configs.put(CSqlTransform.FLATTEN_MAPS_CONFIG, "true");
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
                assertEquals(1, struct.get("obj_x"));
                assertEquals(2, struct.get("obj_y"));
        }

        @Test
        public void testJsonValueFunctionWithFlattenMapsDisabled() {
                // When flatten.maps is false, use Calcite's JSON_VALUE function to extract nested values
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                        "SELECT name, JSON_VALUE(obj, '$.x') as x_val, JSON_VALUE(obj, '$.y') as y_val FROM inputrecord");
                configs.put(CSqlTransform.FLATTEN_MAPS_CONFIG, "false");
                transform.configure(configs);
                String inputJson = "{\n"
                        + "  \"name\": \"test\",\n"
                        + "  \"obj\": {\n"
                        + "    \"x\": 1,\n"
                        + "    \"y\": 2\n"
                        + "  }\n"
                        + "}";
                SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
                SinkRecord output = transform.apply(record);
                assertNotNull(output);
                Struct struct = (Struct) output.value();
                assertEquals("test", struct.get("name"));
                assertEquals("1", struct.get("x_val"));
                assertEquals("2", struct.get("y_val"));
        }

        @Test
        public void testJsonQueryFunctionForNestedObject() {
                // Use JSON_QUERY to extract a nested object as JSON string
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                        "SELECT id, JSON_QUERY(details, '$.address') as address_json FROM inputrecord");
                configs.put(CSqlTransform.FLATTEN_MAPS_CONFIG, "false");
                transform.configure(configs);
                String inputJson = "{\n"
                        + "  \"id\": 123,\n"
                        + "  \"details\": {\n"
                        + "    \"name\": \"John\",\n"
                        + "    \"address\": {\n"
                        + "      \"city\": \"NYC\",\n"
                        + "      \"zip\": \"10001\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
                SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
                SinkRecord output = transform.apply(record);
                assertNotNull(output);
                Struct struct = (Struct) output.value();
                assertEquals(123, struct.get("id"));
                // JSON_QUERY returns the nested object as a JSON string
                String addressJson = (String) struct.get("address_json");
                assertNotNull(addressJson);
                assertTrue(addressJson.contains("NYC"));
                assertTrue(addressJson.contains("10001"));
        }

        @Test
        public void testJsonExistsFunctionForConditionalLogic() {
                // Use JSON_EXISTS to check if a path exists in the JSON
                CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
                Map<String, Object> configs = new HashMap<>();
                configs.put(CSqlTransform.STATEMENT_CONFIG,
                        "SELECT id, JSON_EXISTS(metadata, '$.priority') as has_priority FROM inputrecord");
                configs.put(CSqlTransform.FLATTEN_MAPS_CONFIG, "false");
                transform.configure(configs);
                String inputJson = "{\n"
                        + "  \"id\": 456,\n"
                        + "  \"metadata\": {\n"
                        + "    \"priority\": \"high\",\n"
                        + "    \"tags\": [\"urgent\"]\n"
                        + "  }\n"
                        + "}";
                SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
                SinkRecord output = transform.apply(record);
                assertNotNull(output);
                Struct struct = (Struct) output.value();
                assertEquals(456, struct.get("id"));
                assertEquals(true, struct.get("has_priority"));
        }

}
