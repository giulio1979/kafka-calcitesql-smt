
package com.example.kafkacsqlsmt;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.RelBuilder;
import java.sql.DriverManager;
import java.sql.Connection;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigDef;

public class CSqlTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String STATEMENT_CONFIG = "kafka.connect.transform.csql.statement";
    private String statement;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs) {

        this.statement = (String) configs.get(STATEMENT_CONFIG);
    }

    @Override
    public R apply(R record) {
        try {
            Object value = record.value();
            Map<String, Object> jsonMap;
            if (value instanceof String) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = objectMapper.readValue((String) value, Map.class);
                jsonMap = map;
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                jsonMap = map;
            } else if (value instanceof Struct) {
                jsonMap = structToMap((Struct) value);
            } else {
                throw new DataException("Unsupported record value type: " + value.getClass());
            }

            // Use Calcite to execute SQL statement on jsonMap
            // Create in-memory schema and table
            Connection connection = DriverManager.getConnection("jdbc:calcite:");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            rootSchema.add("inputrecord", Frameworks.createRootSchema(false));

            // Create a single-row table from jsonMap
            SimpleCalciteTable table = new SimpleCalciteTable(jsonMap);
            rootSchema.add("inputrecord", table);

            // Parse and execute SQL
            SqlParser sqlParser = SqlParser.create(statement);
            sqlParser.parseStmt();
            RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
            relBuilder.scan("inputrecord");
            // For now, only support SELECT *
            // TODO: Support arbitrary SELECT statements
            table.getRow();


            // Build output schema
            // Build output schema and struct based on SQL statement fields
            // For now, parse fields from SQL: select a, b, ... from inputrecord
            String fieldList = statement.replaceAll("(?i)^select|from.*$", "").trim();
            String[] fields = fieldList.split(",");
            SchemaBuilder builder = SchemaBuilder.struct();
            for (String field : fields) {
                String fieldName = field.trim().split(" ")[0];
                Object fieldValue = jsonMap.getOrDefault(fieldName, null);
                builder.field(fieldName, inferSchema(fieldValue));
            }
            Schema outputSchema = builder.build();
            Struct outputStruct = new Struct(outputSchema);
            for (String field : fields) {
                String fieldName = field.trim().split(" ")[0];
                Object fieldValue = jsonMap.getOrDefault(fieldName, null);
                if (fieldValue instanceof Map || fieldValue instanceof java.util.List) {
                    try {
                        fieldValue = objectMapper.writeValueAsString(fieldValue);
                    } catch (Exception e) {
                        fieldValue = null;
                    }
                }
                outputStruct.put(fieldName, fieldValue);
            }

            // Return new record with output schema and value
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                outputSchema,
                outputStruct,
                record.timestamp()
            );

        } catch (Exception e) {
            throw new DataException("Failed to apply CSqlTransform", e);
        }
    }

    // Helper to infer Kafka Connect Schema from Java Object
    private Schema inferSchema(Object value) {
    if (value == null) return Schema.OPTIONAL_STRING_SCHEMA;
    if (value instanceof Integer) return Schema.INT32_SCHEMA;
    if (value instanceof Long) return Schema.INT64_SCHEMA;
    if (value instanceof Float) return Schema.FLOAT32_SCHEMA;
    if (value instanceof Double) return Schema.FLOAT64_SCHEMA;
    if (value instanceof Boolean) return Schema.BOOLEAN_SCHEMA;
    if (value instanceof String) return Schema.STRING_SCHEMA;
    if (value instanceof Map) return Schema.STRING_SCHEMA; // Serialize nested objects as JSON string
    if (value instanceof java.util.List) return Schema.STRING_SCHEMA; // Serialize arrays as JSON string
    return Schema.OPTIONAL_STRING_SCHEMA;
    }

    // Simple in-memory table for Calcite
    static class SimpleCalciteTable extends org.apache.calcite.schema.impl.AbstractTable {
        private final Map<String, Object> row;
        public SimpleCalciteTable(Map<String, Object> row) {
            this.row = row;
        }
        public Map<String, Object> getRow() {
            return row;
        }
        @Override
        public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
            final org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                builder.add(entry.getKey(), typeFactory.createJavaType(entry.getValue() != null ? entry.getValue().getClass() : Object.class));
            }
            return builder.build();
        }
    }

    private Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            map.put(field.name(), struct.get(field));
        }
        return map;
    }

    @Override
    public void close() {}

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(STATEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "SQL statement to execute");
    }
}
