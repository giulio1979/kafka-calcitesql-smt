package com.example.kafkacsqlsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;

public class CSqlTransformConfig {
    public static final String STATEMENT_CONFIG = "kafka.connect.transform.csql.statement";
    public static final String SKIP_BYTES_CONFIG = "kafka.connect.transform.csql.skip.bytes";
    public static final String SKIP_BYTES_ENABLED_CONFIG = "kafka.connect.transform.csql.skip.bytes.enabled";
    
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(STATEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, 
                    "SQL statement to execute")
            .define(SKIP_BYTES_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, 
                    "Enable skipping bytes at the beginning of byte array messages (useful for broken JSONSchemaConverter with schema registry)")
            .define(SKIP_BYTES_CONFIG, ConfigDef.Type.INT, 5, ConfigDef.Importance.LOW, 
                    "Number of bytes to skip when skip.bytes.enabled is true (default: 5 for JSONSchemaConverter magic byte + schema ID)");
    }
}
