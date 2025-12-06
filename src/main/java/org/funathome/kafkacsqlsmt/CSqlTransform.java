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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import java.sql.DriverManager;
import java.sql.Connection;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigDef;

public class CSqlTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(CSqlTransform.class);
    public static final String STATEMENT_CONFIG = "kafka.connect.transform.csql.statement";
    public static final String SKIP_BYTES_CONFIG = "kafka.connect.transform.csql.skip.bytes";
    public static final String SKIP_BYTES_ENABLED_CONFIG = "kafka.connect.transform.csql.skip.bytes.enabled";
    public static final String DEFAULT_MESSAGE_CONFIG = "kafka.connect.transform.csql.default.message";
    public static final String FLATTEN_MAPS_CONFIG = "kafka.connect.transform.csql.flatten.maps";
    private static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_TYPE = new TypeReference<Map<String, Object>>() {};
    private String statement;
    private int skipBytes = 5; // Default: 5 bytes for JSONSchemaConverter (1 magic byte + 4 schema ID)
    private boolean skipBytesEnabled = false;
    private boolean flattenMaps = true; // Default: flatten nested maps
    private Map<String, Object> defaultMessage = null;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs) {
        this.statement = (String) configs.get(STATEMENT_CONFIG);
        this.skipBytesEnabled = configs.containsKey(SKIP_BYTES_ENABLED_CONFIG) ? 
            Boolean.parseBoolean(configs.get(SKIP_BYTES_ENABLED_CONFIG).toString()) : false;
        if (configs.containsKey(SKIP_BYTES_CONFIG)) {
            this.skipBytes = Integer.parseInt(configs.get(SKIP_BYTES_CONFIG).toString());
        }
        
        // Parse flatten maps option (default: true)
        this.flattenMaps = configs.containsKey(FLATTEN_MAPS_CONFIG) ?
            Boolean.parseBoolean(configs.get(FLATTEN_MAPS_CONFIG).toString()) : true;
        
        // Parse default message JSON if provided
        if (configs.containsKey(DEFAULT_MESSAGE_CONFIG)) {
            String defaultMessageJson = (String) configs.get(DEFAULT_MESSAGE_CONFIG);
            try {
                this.defaultMessage = readJsonMap(defaultMessageJson);
                log.info("Default message configured: {}", defaultMessage);
            } catch (Exception e) {
                throw new DataException("Failed to parse default message JSON: " + defaultMessageJson, e);
            }
        }
        
        log.info("CSqlTransform configured: skipBytesEnabled={}, skipBytes={}, flattenMaps={}, hasDefaultMessage={}", 
                skipBytesEnabled, skipBytes, flattenMaps, defaultMessage != null);
    }

    @Override
    public R apply(R record) {
        try {
            Object value = record.value();
            if (value == null) {
                throw new DataException("Record value is null; cannot apply SQL transformation");
            }
            log.debug("CSqlTransform received value type: {}", value.getClass().getName());
            Map<String, Object> jsonMap;
            
            // Handle byte array with skip bytes feature for broken JSONSchemaConverter
            if (skipBytesEnabled && value instanceof byte[]) {
                byte[] bytes = (byte[]) value;
                log.debug("Processing byte array with skip bytes enabled. Array length: {}, bytes to skip: {}", bytes.length, skipBytes);
                if (bytes.length <= skipBytes) {
                    throw new DataException("Byte array too short to skip " + skipBytes + " bytes. Length: " + bytes.length);
                }
                // Skip the first N bytes (schema registry magic byte + schema ID)
                String jsonString = new String(bytes, skipBytes, bytes.length - skipBytes, java.nio.charset.StandardCharsets.UTF_8);
                log.debug("Skipped {} bytes from byte array, parsing remaining as JSON: {}", skipBytes, jsonString);
                jsonMap = readJsonMap(jsonString);
            }  else if (value instanceof String) {
                jsonMap = readJsonMap((String) value);
            } else if (value instanceof Map) {
                jsonMap = sanitizeMap((Map<?, ?>) value);
            } else if (value instanceof Struct) {
                jsonMap = structToMap((Struct) value);
            } else if (value instanceof byte[]) {
                // Byte array without skip bytes enabled - treat as UTF-8 JSON string
                String jsonString = new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8);
                jsonMap = readJsonMap(jsonString);
            } else {
                throw new DataException("Unsupported record value type: " + value.getClass());
            }
            
            // Deep merge default message if configured
            if (defaultMessage != null) {
                log.debug("CSqlTransform DEFAULTS: {}", defaultMessage);
                log.debug("CSqlTransform DEFAULT MESSAGE field count: {}, fields: {}", defaultMessage.size(), defaultMessage.keySet());
                log.debug("CSqlTransform INPUT BEFORE MERGE field count: {}, fields: {}", jsonMap.size(), jsonMap.keySet());
                jsonMap = deepMerge(defaultMessage, jsonMap);
                log.debug("CSqlTransform MERGED RECORD field count: {}, fields: {}", jsonMap.size(), jsonMap.keySet());
                
            }

            // Flatten nested structures: maps become prefixed keys, arrays become JSON strings
            jsonMap = flattenMap(jsonMap, "");            

            log.debug("CSqlTransform INPUT RECORD (flattened): {}", jsonMap);
            log.debug("CSqlTransform INPUT SCHEMA: {}", record.valueSchema());
            log.debug("CSqlTransform SQL STATEMENT: {}", statement);

            // CRITICAL: Switch to system classloader BEFORE any Calcite operations
            // Kafka Connect's isolated plugin classloader breaks Janino's ability to find JDK classes
            // like java.lang.String, causing "findIClass("LString;")" errors in production.
            // The classloader must be switched BEFORE DriverManager.getConnection() because
            // Calcite/Janino capture the classloader during connection initialization.
            ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            SchemaBuilder builder = SchemaBuilder.struct();
            Struct outputStruct;
            
            try {
                Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
                
                // Register tables based on SQL statement analysis
                java.util.Properties props = new java.util.Properties();
                props.setProperty("caseSensitive", "false");
                props.setProperty("quotedCasing", "UNCHANGED");
                props.setProperty("unquotedCasing", "UNCHANGED");
                
                Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
                CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
                SchemaPlus rootSchema = calciteConnection.getRootSchema();

                rootSchema.add("inputrecord", new SimpleCalciteTable(jsonMap));
                log.debug("CSqlTransform REGISTERED TABLE: inputrecord");
                
                java.sql.Statement stmt = calciteConnection.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery(statement);
                java.sql.ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                // Build output schema from SQL query results metadata
                for (int i = 1; i <= columnCount; i++) {
                    String colName = meta.getColumnLabel(i);
                    int colType = meta.getColumnType(i);
                    builder.field(colName, sqlTypeToConnectSchema(colType));
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
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
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

    /**
     * Flatten a nested map structure for use with Calcite.
     * If flattenMaps is true: nested maps become prefixed keys (e.g., address.city -> address_city).
     * If flattenMaps is false: nested maps are serialized to JSON strings.
     * Arrays and lists are always serialized to JSON strings.
     * 
     * @param input The input map to flatten
     * @param prefix The current key prefix (empty string for root level)
     * @return A flattened map with only primitive values
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenMap(Map<String, Object> input, String prefix) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "_" + entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof Map) {
                if (flattenMaps) {
                    // Recursively flatten nested maps
                    result.putAll(flattenMap((Map<String, Object>) value, key));
                } else {
                    // Serialize map to JSON string
                    try {
                        result.put(key, objectMapper.writeValueAsString(value));
                    } catch (Exception e) {
                        result.put(key, String.valueOf(value));
                    }
                }
            } else if (value instanceof java.util.List || (value != null && value.getClass().isArray())) {
                // Arrays/lists become JSON strings
                try {
                    result.put(key, objectMapper.writeValueAsString(value));
                } catch (Exception e) {
                    result.put(key, String.valueOf(value));
                }
            } else {
                // Primitives pass through as-is
                result.put(key, value);
            }
        }
        return result;
    }

    /**
     * Deep merge two maps, with values from 'source' overriding values in 'target'.
     * For nested maps, recursively merges them. For other values, source wins.
     * 
     * @param target The base map (default values)
     * @param source The override map (actual input)
     * @return A new merged map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> deepMerge(Map<String, Object> target, Map<String, Object> source) {
        Map<String, Object> result = new HashMap<>(target);
        
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object sourceValue = entry.getValue();
            
            if (sourceValue == null) {
                // Keep the value from source even if it's null (explicit override)
                result.put(key, null);
            } else if (result.containsKey(key) && result.get(key) instanceof Map && sourceValue instanceof Map) {
                // Both are maps, recursively merge
                result.put(key, deepMerge((Map<String, Object>) result.get(key), (Map<String, Object>) sourceValue));
            } else {
                // Source value wins (primitive, array, or different types)
                result.put(key, sourceValue);
            }
        }
        
        return result;
    }

    // Simple in-memory table for Calcite
    static class SimpleCalciteTable extends org.apache.calcite.schema.impl.AbstractTable
            implements org.apache.calcite.schema.ScannableTable {
        private final Map<String, Object> row;
        private final java.util.List<String> fieldNames;

        public SimpleCalciteTable(Map<String, Object> row) {
            this.row = row;
            this.fieldNames = new java.util.ArrayList<>(row.keySet());
        }

        @Override
        public org.apache.calcite.linq4j.Enumerable<Object[]> scan(org.apache.calcite.DataContext dataContext) {
            // Extract values in the SAME ORDER as the schema defines them (by field name)
            // This ensures Calcite's generated code accesses the correct array positions
            Object[] values = new Object[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                values[i] = row.get(fieldNames.get(i));
            }
            java.util.List<Object[]> rows = java.util.Collections.singletonList(values);
            return org.apache.calcite.linq4j.Linq4j.asEnumerable(rows);
        }

        @Override
        public org.apache.calcite.rel.type.RelDataType getRowType(
                org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
            final org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder = new org.apache.calcite.rel.type.RelDataTypeFactory.Builder(
                    typeFactory);
            for (String key : fieldNames) {
                Object value = row.get(key);
                builder.add(key, inferType(typeFactory, value));
            }
            return builder.build();
        }

        // Infer SQL type from Java value. Since input is flattened, we only handle primitives.
        private org.apache.calcite.rel.type.RelDataType inferType(
                org.apache.calcite.rel.type.RelDataTypeFactory typeFactory,
                Object value) {
            org.apache.calcite.sql.type.SqlTypeName typeName;
            if (value == null || value instanceof String) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
            } else if (value instanceof Integer) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.INTEGER;
            } else if (value instanceof Long) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.BIGINT;
            } else if (value instanceof Float) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.REAL;
            } else if (value instanceof Double) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
            } else if (value instanceof Boolean) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
            } else if (value instanceof java.math.BigDecimal) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
            } else if (value instanceof java.math.BigInteger) {
                typeName = org.apache.calcite.sql.type.SqlTypeName.BIGINT;
            } else {
                typeName = org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
            }
            return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(typeName), true);
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
                return Schema.OPTIONAL_INT32_SCHEMA;
            case java.sql.Types.BIGINT:
                return Schema.OPTIONAL_INT64_SCHEMA;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case java.sql.Types.DOUBLE:
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case java.sql.Types.BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
                return Schema.OPTIONAL_STRING_SCHEMA;
            default:
                return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(STATEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "SQL statement to execute")
                .define(SKIP_BYTES_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, 
                        "Enable skipping bytes at the beginning of byte array messages (useful for broken JSONSchemaConverter with schema registry)")
                .define(SKIP_BYTES_CONFIG, ConfigDef.Type.INT, 5, ConfigDef.Importance.LOW, 
                        "Number of bytes to skip when skip.bytes.enabled is true (default: 5 for JSONSchemaConverter magic byte + schema ID)")
                .define(FLATTEN_MAPS_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "When true, nested maps are flattened with underscore-separated keys (e.g., address_city). When false, nested maps are serialized as JSON strings.");
    }

}
