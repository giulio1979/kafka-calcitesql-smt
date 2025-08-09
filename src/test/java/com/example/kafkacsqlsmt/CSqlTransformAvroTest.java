package com.example.kafkacsqlsmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class CSqlTransformAvroTest {
    @Test
    public void testAvroInput() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(CSqlTransform.STATEMENT_CONFIG, "select a, b from inputrecord");
        transform.configure(configs);

        String avroSchemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}]}";
        org.apache.avro.Schema avroSchema = new Parser().parse(avroSchemaStr);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("a", 42);
        avroRecord.put("b", "hello");

        // Simulate Avro record as Map for the SMT
        Map<String, Object> avroMap = new HashMap<>();
        avroMap.put("a", avroRecord.get("a"));
        avroMap.put("b", avroRecord.get("b"));
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, avroMap, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();
        assertEquals(42, struct.get("a"));
        assertEquals("hello", struct.get("b"));
    }
}
