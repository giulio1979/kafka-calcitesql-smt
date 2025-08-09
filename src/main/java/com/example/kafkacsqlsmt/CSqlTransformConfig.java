package com.example.kafkacsqlsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Transformation;

public class CSqlTransformConfig {
    public static final String STATEMENT_CONFIG = "kafka.connect.transform.csql.statement";
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(STATEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "SQL statement to execute");
    }
}
