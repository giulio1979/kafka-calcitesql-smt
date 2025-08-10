
package org.funathome.kafkacsqlsmt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(CSqlTransform.class);
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
                jsonMap = objectMapper.readValue((String) value, Map.class);
            } else if (value instanceof Map) {
                jsonMap = (Map<String, Object>) value;
            } else if (value instanceof Struct) {
                jsonMap = structToMap((Struct) value);
            } else {
                throw new DataException("Unsupported record value type: " + value.getClass());
            }

            log.info("CSqlTransform INPUT RECORD: {}", jsonMap);
            log.info("CSqlTransform INPUT SCHEMA: {}", record.valueSchema());
            log.info("CSqlTransform SQL STATEMENT: {}", statement);

            // Register tables based on SQL statement analysis
            java.util.Properties props = new java.util.Properties();
            props.setProperty("caseSensitive", "false");
            props.setProperty("quotedCasing", "UNCHANGED");
            props.setProperty("unquotedCasing", "UNCHANGED");
            Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            // Always register the main table
            rootSchema.add("inputrecord", new SimpleCalciteTable(jsonMap));

            // Parse SQL for table references after FROM and JOIN
            java.util.Set<String> tableRefs = new java.util.HashSet<>();
            if (statement != null) {
                String lowerSql = statement.toLowerCase();
                String[] keywords = {"from", "join"};
                for (String keyword : keywords) {
                    int idx = 0;
                    while ((idx = lowerSql.indexOf(keyword, idx)) != -1) {
                        int start = idx + keyword.length();
                        // Skip whitespace
                        while (start < lowerSql.length() && Character.isWhitespace(lowerSql.charAt(start))) start++;
                        // Read until next whitespace, comma, parenthesis, or end
                        int end = start;
                        while (end < lowerSql.length() &&
                            !Character.isWhitespace(lowerSql.charAt(end)) &&
                            lowerSql.charAt(end) != ',' &&
                            lowerSql.charAt(end) != '(' &&
                            lowerSql.charAt(end) != ')') {
                            end++;
                        }
                        String ref = statement.substring(start, end).replaceAll("[\"']", "");
                        if (ref.startsWith("inputrecord.")) {
                            tableRefs.add(ref);
                        }
                        idx = end;
                    }
                }
            }

            // Register only referenced subtables
            for (String ref : tableRefs) {
                String fieldName = ref.substring("inputrecord.".length());
                Object subValue = jsonMap.get(fieldName);
                if (subValue == null) continue;
                if (subValue instanceof java.util.List) {
                    rootSchema.add(ref, new SimpleCalciteArrayTable((java.util.List<?>) subValue));
                    log.info("CSqlTransform: Registered subtable '{}' as array (from List)", ref);
                } else if (subValue instanceof String) {
                    try {
                        Object parsed = objectMapper.readValue((String) subValue, Object.class);
                        if (parsed instanceof java.util.List) {
                            java.util.List<?> parsedList = (java.util.List<?>) parsed;
                            if (!parsedList.isEmpty() && parsedList.get(0) instanceof Map) {
                                rootSchema.add(ref, new SimpleCalciteArrayTable(parsedList));
                                jsonMap.put(fieldName, parsedList);
                                log.info("CSqlTransform: Registered subtable '{}' as array of objects (from string) and updated main record", ref);
                            } else {
                                log.warn("CSqlTransform: Parsed string field '{}' as array, but not array of objects, skipping registration", fieldName);
                            }
                        } else if (parsed instanceof Map) {
                            java.util.List<Map<String, Object>> singleRowList = new java.util.ArrayList<>();
                            singleRowList.add((Map<String, Object>) parsed);
                            rootSchema.add(ref, new SimpleCalciteArrayTable(singleRowList));
                            jsonMap.put(fieldName, parsed);
                            log.info("CSqlTransform: Registered subtable '{}' as single-row object (from string/inner table) and updated main record", ref);
                        } else {
                            log.warn("CSqlTransform: Parsed string field '{}' but result is not array or object, skipping registration", fieldName);
                        }
                    } catch (Exception ex) {
                        log.warn("CSqlTransform: Could not parse string field '{}' as JSON array/object: {}", fieldName, ex.getMessage());
                    }
                } else if (subValue instanceof Map) {
                    java.util.List<Map<String, Object>> singleRowList = new java.util.ArrayList<>();
                    singleRowList.add((Map<String, Object>) subValue);
                    rootSchema.add(ref, new SimpleCalciteArrayTable(singleRowList));
                    log.info("CSqlTransform: Registered subtable '{}' as single-row object (from Map)", ref);
                }
            }

            for (String tableName : rootSchema.getTableNames()) {
                log.info("CSqlTransform REGISTERED TABLE: {} type={}", tableName,
                        rootSchema.getTable(tableName).getClass().getName());
            }

            SchemaBuilder builder = SchemaBuilder.struct();
            Struct outputStruct;
            try {
                java.sql.Statement stmt = calciteConnection.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery(statement);
                java.sql.ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String colName = meta.getColumnLabel(i);
                    // For join queries, strip alias prefix if present (dot or underscore)
                    String baseName = colName;
                    if (baseName.contains(".")) {
                        String[] parts = baseName.split("\\.");
                        if (parts.length == 2 && (parts[0].equals("a") || parts[0].equals("b"))) {
                            baseName = parts[1];
                        }
                    } else if (baseName.contains("_")) {
                        String[] parts = baseName.split("_");
                        if (parts.length > 1 && (parts[0].equals("a") || parts[0].equals("b"))) {
                            baseName = String.join("_", java.util.Arrays.copyOfRange(parts, 1, parts.length));
                        }
                    }
                    int colType = meta.getColumnType(i);
                    builder.field(baseName, sqlTypeToConnectSchema(colType));
                }
                Schema outputSchema = builder.build();
                outputStruct = new Struct(outputSchema);
                if (rs.next()) {
                    for (org.apache.kafka.connect.data.Field field : outputSchema.fields()) {
                        Object fieldValue = rs.getObject(field.name());
                        if (fieldValue instanceof Map || fieldValue instanceof java.util.List) {
                            fieldValue = objectMapper.writeValueAsString(fieldValue);
                        }
                        outputStruct.put(field.name(), fieldValue);
                    }
                }
                log.debug("CSqlTransform OUTPUT SCHEMA: {}", outputSchema);
                log.debug("CSqlTransform OUTPUT STRUCT: {}", outputStruct);
                rs.close();
                stmt.close();
                connection.close();
            } catch (java.sql.SQLException sqlEx) {
                log.error("CSqlTransform error: {}", sqlEx.getMessage(), sqlEx);
                throw new DataException("Failed to apply CSqlTransform", sqlEx);
            }

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    outputStruct.schema(),
                    outputStruct,
                    record.timestamp());
        } catch (Exception e) {
            log.error("CSqlTransform error: {}", e.getMessage(), e);
            throw new DataException("Failed to apply CSqlTransform", e);
        }
    }

    // Helper to infer Kafka Connect Schema from Java Object
    private Schema inferSchema(Object value) {
        // Helper to convert SQL type to Kafka Connect Schema
        if (value == null)
            return Schema.OPTIONAL_STRING_SCHEMA;
        if (value instanceof Integer)
            return Schema.INT32_SCHEMA;
        if (value instanceof Long)
            return Schema.INT64_SCHEMA;
        if (value instanceof Float)
            return Schema.FLOAT32_SCHEMA;
        if (value instanceof Double)
            return Schema.FLOAT64_SCHEMA;
        if (value instanceof Boolean)
            return Schema.BOOLEAN_SCHEMA;
        if (value instanceof String)
            return Schema.STRING_SCHEMA;
        if (value instanceof Map)
            return Schema.STRING_SCHEMA; // Serialize nested objects as JSON string
        if (value instanceof java.util.List)
            return Schema.STRING_SCHEMA; // Serialize arrays as JSON string
        return Schema.OPTIONAL_STRING_SCHEMA;
    }

    // Simple in-memory table for Calcite
    static class SimpleCalciteTable extends org.apache.calcite.schema.impl.AbstractTable
            implements org.apache.calcite.schema.ScannableTable {
        private final Map<String, Object> row;

        public SimpleCalciteTable(Map<String, Object> row) {
            this.row = row;
        }

        @Override
        public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext dataContext) {
            Object[] values = row.values().toArray();
            java.util.List<Object[]> rows = java.util.Collections.singletonList(values);
            return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
        }

        @Override
        public org.apache.calcite.rel.type.RelDataType getRowType(
                org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
            final org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = new org.apache.calcite.rel.type.RelDataTypeFactory.Builder(
                    typeFactory);
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                builder.add(entry.getKey(), typeFactory
                        .createJavaType(entry.getValue() != null ? entry.getValue().getClass() : Object.class));
            }
            return builder.build();
        }
    }

    // Table for nested arrays (for joins)
    static class SimpleCalciteArrayTable extends org.apache.calcite.schema.impl.AbstractTable
            implements org.apache.calcite.schema.ScannableTable {
        private final java.util.List<?> array;

        public SimpleCalciteArrayTable(java.util.List<?> array) {
            this.array = array;
        }

        @Override
        public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext dataContext) {
            java.util.List<Object[]> rows = new java.util.ArrayList<>();
            for (Object item : array) {
                if (item instanceof Map) {
                    Map<?, ?> map = (Map<?, ?>) item;
                    rows.add(map.values().toArray());
                } else {
                    rows.add(new Object[] { item });
                }
            }
            return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
        }

        @Override
        public org.apache.calcite.rel.type.RelDataType getRowType(
                org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
            final org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = new org.apache.calcite.rel.type.RelDataTypeFactory.Builder(
                    typeFactory);
            if (!array.isEmpty() && array.get(0) instanceof Map) {
                Map<?, ?> first = (Map<?, ?>) array.get(0);
                for (Map.Entry<?, ?> entry : first.entrySet()) {
                    builder.add(entry.getKey().toString(), typeFactory
                            .createJavaType(entry.getValue() != null ? entry.getValue().getClass() : Object.class));
                }
            } else {
                builder.add("value",
                        typeFactory.createJavaType(array.get(0) != null ? array.get(0).getClass() : Object.class));
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

    // Helper to convert SQL type to Kafka Connect Schema
    private Schema sqlTypeToConnectSchema(int sqlType) {
        switch (sqlType) {
            case java.sql.Types.INTEGER:
                return Schema.INT32_SCHEMA;
            case java.sql.Types.BIGINT:
                return Schema.INT64_SCHEMA;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return Schema.FLOAT32_SCHEMA;
            case java.sql.Types.DOUBLE:
                return Schema.FLOAT64_SCHEMA;
            case java.sql.Types.BOOLEAN:
                return Schema.BOOLEAN_SCHEMA;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
                return Schema.STRING_SCHEMA;
            default:
                return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    // Helper to convert SQL type to Kafka Connect Schema

    // Helper to convert SQL type to Kafka Connect Schema

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(STATEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "SQL statement to execute");
    }
}
