package org.funathome.kafkacsqlsmt;

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


    @Test
    public void testCustomerOrderAnalyticsWithAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        // Define Avro schema for customer order analytics data
        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"CustomerOrderAnalytics\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"order_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_email\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_phone\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"product_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"product_category\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"product_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"brand_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"brand_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"tags\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"order_date\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"payment_method_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"payment_method_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"quantity\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
                "    {\"name\": \"unit_price\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"total_amount\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"shipping_cost\", \"type\": [\"null\", \"float\"], \"default\": null},\n" +
                "    {\"name\": \"tax_amount\", \"type\": [\"null\", \"float\"], \"default\": null},\n" +
                "    {\"name\": \"discount_code\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"discount_percent\", \"type\": [\"null\", \"float\"], \"default\": null},\n" +
                "    {\"name\": \"promo_campaign_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"promo_campaign_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"warehouse_location\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"warehouse_region\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"delivery_date\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"tracking_number\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"sales_rep_first_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"sales_rep_last_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"store_location\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"store_region\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"sales_rep_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"store_id\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "select " +
                        "  cast(COALESCE(order_id, NULL) as varchar) as order_id, " +
                        "  cast(COALESCE(customer_id, NULL) as varchar) as customer_id, " +
                        "  cast(COALESCE(customer_email, NULL) as varchar) as customer_email, " +
                        "  cast(COALESCE(customer_phone, NULL) as varchar) as customer_phone, " +
                        "  cast(COALESCE(product_id, NULL) as varchar) as product_id, " +
                        "  cast(COALESCE(product_category, NULL) as int) as product_category, " +
                        "  cast(COALESCE(product_name, NULL) as varchar) as product_name, " +
                        "  cast(COALESCE(brand_id, NULL) as varchar) as brand_id, " +
                        "  cast(COALESCE(brand_name, NULL) as varchar) as brand_name, " +
                        "  COALESCE(tags, NULL) as tags, " +
                        "  cast(COALESCE(order_date, NULL) as varchar) as order_date, " +
                        "  cast(COALESCE(payment_method_id, NULL) as varchar) as payment_method_id, " +
                        "  cast(COALESCE(payment_method_name, NULL) as varchar) as payment_method_name, " +
                        "  cast(COALESCE(quantity, NULL) as bigint) as quantity, " +
                        "  cast(COALESCE(unit_price, NULL) as double) as unit_price, " +
                        "  cast(COALESCE(total_amount, NULL) as double) as total_amount, " +
                        "  cast(COALESCE(shipping_cost, NULL) as float) as shipping_cost, " +
                        "  cast(COALESCE(tax_amount, NULL) as float) as tax_amount, " +
                        "  cast(COALESCE(discount_code, NULL) as varchar) as discount_code, " +
                        "  cast(COALESCE(discount_percent, NULL) as float) as discount_percent, " +
                        "  cast(COALESCE(promo_campaign_id, NULL) as varchar) as promo_campaign_id, " +
                        "  cast(COALESCE(promo_campaign_name, NULL) as varchar) as promo_campaign_name, " +
                        "  cast(COALESCE(warehouse_location, NULL) as varchar) as warehouse_location, " +
                        "  cast(COALESCE(warehouse_region, NULL) as varchar) as warehouse_region, " +
                        "  cast(COALESCE(delivery_date, NULL) as varchar) as delivery_date, " +
                        "  cast(COALESCE(tracking_number, NULL) as varchar) as tracking_number, " +
                        "  cast(COALESCE(sales_rep_first_name, NULL) as varchar) as sales_rep_first_name, " +
                        "  cast(COALESCE(sales_rep_last_name, NULL) as varchar) as sales_rep_last_name, " +
                        "  cast(COALESCE(store_location, NULL) as varchar) as store_location, " +
                        "  cast(COALESCE(store_region, NULL) as varchar) as store_region, " +
                        "  cast(COALESCE(sales_rep_id, NULL) as varchar) as sales_rep_id, " +
                        "  cast(COALESCE(store_id, NULL) as varchar) as store_id " +
                        "from inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);

        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"order_id\": \"ORD-45782\",\n"
                + "  \"customer_id\": \"CUST-9921\",\n"
                + "  \"customer_email\": \"sarah.johnson@email.com\",\n"
                + "  \"customer_phone\": \"+1-555-0123\",\n"
                + "  \"sales_rep_id\": \"REP-7834\",\n"
                + "  \"sales_rep_first_name\": \"Michael\",\n"
                + "  \"sales_rep_last_name\": \"Chen\",\n"
                + "  \"product_id\": \"PROD-3421\",\n"
                + "  \"product_category\": 15,\n"
                + "  \"product_name\": \"Wireless Bluetooth Headphones\",\n"
                + "  \"brand_id\": \"BRAND-88\",\n"
                + "  \"brand_name\": \"TechSound\",\n"
                + "  \"tags\": [\"electronics\", \"audio\", \"wireless\"],\n"
                + "  \"order_date\": \"2025-08-16T14:32:15Z\",\n"
                + "  \"payment_method_id\": \"PAY-12\",\n"
                + "  \"payment_method_name\": \"Credit Card\",\n"
                + "  \"quantity\": 2,\n"
                + "  \"unit_price\": 89.99,\n"
                + "  \"total_amount\": 179.98,\n"
                + "  \"store_id\": \"STORE-NYC-001\",\n"
                + "  \"store_location\": \"Manhattan Downtown\",\n"
                + "  \"store_region\": \"North America\",\n"
                + "  \"tracking_number\": \"TRK-8829471\",\n"
                + "  \"delivery_date\": \"2025-08-18T16:00:00Z\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        // Verify all the main fields are present
        assertEquals("ORD-45782", struct.get("order_id"));
        assertEquals("CUST-9921", struct.get("customer_id"));
        assertEquals("sarah.johnson@email.com", struct.get("customer_email"));
        assertEquals("+1-555-0123", struct.get("customer_phone"));
        assertEquals("PROD-3421", struct.get("product_id"));
        assertEquals(15, struct.get("product_category"));
        assertEquals("Wireless Bluetooth Headphones", struct.get("product_name"));
        assertEquals("BRAND-88", struct.get("brand_id"));
        assertEquals("TechSound", struct.get("brand_name"));
        assertEquals("2025-08-16T14:32:15Z", struct.get("order_date"));
        assertEquals("PAY-12", struct.get("payment_method_id"));
        assertEquals("Credit Card", struct.get("payment_method_name"));
        assertEquals(2L, struct.get("quantity"));
        assertEquals(89.99, struct.get("unit_price"));
        assertEquals(179.98, struct.get("total_amount"));
        assertEquals("TRK-8829471", struct.get("tracking_number"));
        assertEquals("2025-08-18T16:00:00Z", struct.get("delivery_date"));
        assertEquals("Michael", struct.get("sales_rep_first_name"));
        assertEquals("Chen", struct.get("sales_rep_last_name"));
        assertEquals("Manhattan Downtown", struct.get("store_location"));
        assertEquals("REP-7834", struct.get("sales_rep_id"));
        assertEquals("STORE-NYC-001", struct.get("store_id"));

        // Verify optional fields are null (as expected for missing data)
        assertNull(struct.get("shipping_cost"));
        assertNull(struct.get("tax_amount"));
        assertNull(struct.get("discount_code"));
        assertNull(struct.get("discount_percent"));
        assertNull(struct.get("promo_campaign_id"));
        assertNull(struct.get("promo_campaign_name"));
        assertNull(struct.get("warehouse_location"));
        assertNull(struct.get("warehouse_region"));

        // Verify store_region has the expected value from input
        assertEquals("North America", struct.get("store_region"));

        // Verify tags array is serialized as string
        assertNotNull(struct.get("tags"));
        String tagsStr = (String) struct.get("tags");
        assertTrue(tagsStr.contains("electronics") || tagsStr.contains("audio") || tagsStr.contains("wireless"));
    }


    @Test
    public void testUserProfileWithJsonFieldsAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"UserProfile\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"user_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"preferences\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"metadata\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"age\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"active\", \"type\": [\"null\", \"boolean\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT user_id, preferences, metadata, age, active FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"user_id\": \"USR-123\",\n"
                + "  \"preferences\": \"{\\\"theme\\\": \\\"dark\\\", \\\"language\\\": \\\"en\\\"}\",\n"
                + "  \"metadata\": \"{\\\"lastLogin\\\": \\\"2025-08-16T10:30:00Z\\\", \\\"deviceType\\\": \\\"mobile\\\"}\",\n"
                + "  \"age\": 28,\n"
                + "  \"active\": true\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("USR-123", struct.get("user_id"));
        String preferences = (String) struct.get("preferences");
        assertTrue(preferences.contains("theme") && preferences.contains("dark"));
        String metadata = (String) struct.get("metadata");
        assertTrue(metadata.contains("lastLogin") && metadata.contains("deviceType"));
        assertEquals(28, struct.get("age"));
        assertEquals(true, struct.get("active"));
    }

    @Test
    public void testOrderItemsJoinFlatteningAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"OrderData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"order_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"items\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT o.order_id, o.customer_name, i.product_name, i.quantity, i.price " +
                        "FROM inputrecord o JOIN \"inputrecord.items\" i ON TRUE");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"order_id\": \"ORD-789\",\n"
                + "  \"customer_name\": \"Alice Smith\",\n"
                + "  \"items\": [\n"
                + "    {\"product_name\": \"Laptop\", \"quantity\": 1, \"price\": 999.99},\n"
                + "    {\"product_name\": \"Mouse\", \"quantity\": 2, \"price\": 25.50}\n"
                + "  ]\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("ORD-789", struct.get("order_id"));
        assertEquals("Alice Smith", struct.get("customer_name"));
        // First item should be selected from the array
        assertNotNull(struct.get("product_name"));
        assertNotNull(struct.get("quantity"));
        assertNotNull(struct.get("price"));
    }

    @Test
    public void testBasicMathOperationsAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"SalesData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"sale_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"quantity\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
                "    {\"name\": \"unit_price\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"tax_rate\", \"type\": [\"null\", \"double\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT sale_id, quantity, unit_price, tax_rate, " +
                        "(quantity * unit_price) as subtotal, " +
                        "(quantity * unit_price * tax_rate) as tax_amount, " +
                        "(quantity * unit_price * (1 + tax_rate)) as total_amount " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"sale_id\": \"SALE-456\",\n"
                + "  \"quantity\": 3,\n"
                + "  \"unit_price\": 15.99,\n"
                + "  \"tax_rate\": 0.08\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("SALE-456", struct.get("sale_id"));
        assertEquals(3, struct.get("quantity"));
        assertEquals(15.99, struct.get("unit_price"));
        assertEquals(0.08, struct.get("tax_rate"));

        // Verify calculated fields
        Double subtotal = (Double) struct.get("subtotal");
        assertTrue(Math.abs(subtotal - 47.97) < 0.01);
        Double taxAmount = (Double) struct.get("tax_amount");
        assertTrue(Math.abs(taxAmount - 3.8376) < 0.01);
        Double totalAmount = (Double) struct.get("total_amount");
        assertTrue(Math.abs(totalAmount - 51.8076) < 0.01);
    }

    @Test
    public void testBooleanLogicOperationsAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"EmployeeData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"employee_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"is_active\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n" +
                "    {\"name\": \"is_manager\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n" +
                "    {\"name\": \"has_benefits\", \"type\": [\"null\", \"boolean\"], \"default\": null},\n" +
                "    {\"name\": \"salary\", \"type\": [\"null\", \"double\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT employee_id, is_active, is_manager, has_benefits, salary, " +
                        "(is_active AND is_manager) as active_manager, " +
                        "(NOT is_active) as inactive, " +
                        "(salary > 50000) as high_earner, " +
                        "(is_active AND has_benefits AND salary > 60000) as premium_employee " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"employee_id\": \"EMP-789\",\n"
                + "  \"is_active\": true,\n"
                + "  \"is_manager\": false,\n"
                + "  \"has_benefits\": true,\n"
                + "  \"salary\": 75000.0\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("EMP-789", struct.get("employee_id"));
        assertEquals(true, struct.get("is_active"));
        assertEquals(false, struct.get("is_manager"));
        assertEquals(true, struct.get("has_benefits"));
        assertEquals(75000.0, struct.get("salary"));

        // Verify boolean calculations
        assertEquals(false, struct.get("active_manager")); // true AND false
        assertEquals(false, struct.get("inactive")); // NOT true
        assertEquals(true, struct.get("high_earner")); // 75000 > 50000
        assertEquals(true, struct.get("premium_employee")); // true AND true AND true
    }

    @Test
    public void testDateTimeParsingAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"EventData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"event_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"created_at\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"updated_at\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"scheduled_date\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT event_id, created_at, updated_at, scheduled_date, " +
                        "SUBSTRING(created_at, 1, 10) as created_date, " +
                        "SUBSTRING(created_at, 12, 8) as created_time, " +
                        "SUBSTRING(scheduled_date, 1, 4) as scheduled_year " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"event_id\": \"EVT-2025\",\n"
                + "  \"created_at\": \"2025-08-16T14:30:00Z\",\n"
                + "  \"updated_at\": \"2025-08-16T15:45:30Z\",\n"
                + "  \"scheduled_date\": \"2025-12-25T09:00:00Z\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("EVT-2025", struct.get("event_id"));
        assertEquals("2025-08-16T14:30:00Z", struct.get("created_at"));
        assertEquals("2025-08-16T15:45:30Z", struct.get("updated_at"));
        assertEquals("2025-12-25T09:00:00Z", struct.get("scheduled_date"));

        // Verify date/time parsing
        assertEquals("2025-08-16", struct.get("created_date"));
        assertEquals("14:30:00", struct.get("created_time"));
        assertEquals("2025", struct.get("scheduled_year"));
    }

    @Test
    public void testNestedJsonObjectParsingAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"ProductData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"product_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"specifications\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"pricing_info\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"availability\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT product_id, specifications, pricing_info, availability FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"product_id\": \"PROD-2025\",\n"
                + "  \"specifications\": \"{\\\"weight\\\": \\\"2.5kg\\\", \\\"dimensions\\\": \\\"30x20x10cm\\\", \\\"color\\\": \\\"black\\\"}\",\n"
                + "  \"pricing_info\": \"{\\\"base_price\\\": 299.99, \\\"discount\\\": 0.15, \\\"currency\\\": \\\"USD\\\"}\",\n"
                + "  \"availability\": \"{\\\"in_stock\\\": true, \\\"quantity\\\": 50, \\\"warehouse\\\": \\\"NYC-01\\\"}\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("PROD-2025", struct.get("product_id"));

        String specs = (String) struct.get("specifications");
        assertTrue(specs.contains("weight") && specs.contains("2.5kg"));

        String pricing = (String) struct.get("pricing_info");
        assertTrue(pricing.contains("base_price") && pricing.contains("299.99"));

        String availability = (String) struct.get("availability");
        assertTrue(availability.contains("in_stock") && availability.contains("true"));
    }

    @Test
    public void testStringManipulationAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"CustomerData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"customer_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"first_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"last_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"phone\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT customer_id, first_name, last_name, email, phone, " +
                        "(first_name || ' ' || last_name) as full_name, " +
                        "UPPER(email) as email_upper, " +
                        "LOWER(last_name) as last_name_lower, " +
                        "CHAR_LENGTH(phone) as phone_length, " +
                        "SUBSTRING(email, 1, POSITION('@' IN email) - 1) as email_username " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"customer_id\": \"CUST-456\",\n"
                + "  \"first_name\": \"John\",\n"
                + "  \"last_name\": \"Doe\",\n"
                + "  \"email\": \"john.doe@example.com\",\n"
                + "  \"phone\": \"+1-555-123-4567\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("CUST-456", struct.get("customer_id"));
        assertEquals("John", struct.get("first_name"));
        assertEquals("Doe", struct.get("last_name"));
        assertEquals("john.doe@example.com", struct.get("email"));
        assertEquals("+1-555-123-4567", struct.get("phone"));

        // Verify string manipulations
        assertEquals("John Doe", struct.get("full_name"));
        assertEquals("JOHN.DOE@EXAMPLE.COM", struct.get("email_upper"));
        assertEquals("doe", struct.get("last_name_lower"));
        assertEquals(15, struct.get("phone_length"));
        assertEquals("john.doe", struct.get("email_username"));
    }

    @Test
    public void testConditionalLogicAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TransactionData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"transaction_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"amount\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
                "    {\"name\": \"transaction_type\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_tier\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"is_weekend\", \"type\": [\"null\", \"boolean\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT transaction_id, amount, transaction_type, customer_tier, is_weekend, " +
                        "CASE WHEN amount > 1000 THEN 'HIGH' WHEN amount > 100 THEN 'MEDIUM' ELSE 'LOW' END as amount_category, " +
                        "CASE WHEN customer_tier = 'GOLD' THEN amount * 0.95 ELSE amount END as discounted_amount, " +
                        "CASE WHEN is_weekend THEN 'WEEKEND' ELSE 'WEEKDAY' END as day_type, " +
                        "CASE WHEN transaction_type = 'REFUND' THEN -amount ELSE amount END as signed_amount " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"transaction_id\": \"TXN-789\",\n"
                + "  \"amount\": 500.0,\n"
                + "  \"transaction_type\": \"PURCHASE\",\n"
                + "  \"customer_tier\": \"GOLD\",\n"
                + "  \"is_weekend\": false\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("TXN-789", struct.get("transaction_id"));
        assertEquals(500.0, struct.get("amount"));
        assertEquals("PURCHASE", struct.get("transaction_type"));
        assertEquals("GOLD", struct.get("customer_tier"));
        assertEquals(false, struct.get("is_weekend"));

        // Verify conditional logic
        assertEquals("MEDIUM", struct.get("amount_category")); // 500 > 100 but < 1000
        assertEquals(475.0, struct.get("discounted_amount")); // 500 * 0.95 for GOLD tier
        assertEquals("WEEKDAY", struct.get("day_type")); // not weekend
        assertEquals(500.0, struct.get("signed_amount")); // positive for PURCHASE
    }

    @Test
    public void testAggregationWithGroupingAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"SalesReport\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"report_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"region\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"sales_data\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT report_id, region, sales_data FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"report_id\": \"RPT-2025\",\n"
                + "  \"region\": \"North America\",\n"
                + "  \"sales_data\": \"[{\\\"product\\\": \\\"laptops\\\", \\\"quantity\\\": 10, \\\"revenue\\\": 15000}, {\\\"product\\\": \\\"phones\\\", \\\"quantity\\\": 25, \\\"revenue\\\": 12500}]\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("RPT-2025", struct.get("report_id"));
        assertEquals("North America", struct.get("region"));

        String salesData = (String) struct.get("sales_data");
        assertTrue(salesData.contains("laptops") && salesData.contains("phones"));
        assertTrue(salesData.contains("15000") && salesData.contains("12500"));
    }

    @Test
    public void testComplexArrayProcessingAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"InventoryData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"warehouse_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"inventory_items\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"last_updated\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT w.warehouse_id, w.last_updated, i.item_code, i.quantity, i.unit_cost, " +
                        "(i.quantity * i.unit_cost) as total_value " +
                        "FROM inputrecord w JOIN \"inputrecord.inventory_items\" i ON TRUE");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"warehouse_id\": \"WH-NYC-001\",\n"
                + "  \"last_updated\": \"2025-08-16T12:00:00Z\",\n"
                + "  \"inventory_items\": [\n"
                + "    {\"item_code\": \"ITM-001\", \"quantity\": 100, \"unit_cost\": 12.50},\n"
                + "    {\"item_code\": \"ITM-002\", \"quantity\": 75, \"unit_cost\": 8.99},\n"
                + "    {\"item_code\": \"ITM-003\", \"quantity\": 200, \"unit_cost\": 5.25}\n"
                + "  ]\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("WH-NYC-001", struct.get("warehouse_id"));
        assertEquals("2025-08-16T12:00:00Z", struct.get("last_updated"));

        // Should have data from first item in array
        assertNotNull(struct.get("item_code"));
        assertNotNull(struct.get("quantity"));
        assertNotNull(struct.get("unit_cost"));
        assertNotNull(struct.get("total_value"));
    }

    @Test
    public void testJsonValueFunctionAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"ProductWithJsonData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"product_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"product_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"metadata\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"features\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT product_id, product_name, metadata, features, " +
                        "JSON_VALUE(metadata, '$.category') as category, " +
                        "JSON_VALUE(metadata, '$.brand') as brand, " +
                        "JSON_VALUE(metadata, '$.price') as price, " +
                        "JSON_VALUE(features, '$.color') as color, " +
                        "JSON_VALUE(features, '$.size') as size, " +
                        "JSON_VALUE(features, '$.warranty_years') as warranty_years " +
                        "FROM inputrecord");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"product_id\": \"PROD-JSON-001\",\n"
                + "  \"product_name\": \"Smart Watch\",\n"
                + "  \"metadata\": \"{\\\"category\\\": \\\"electronics\\\", \\\"brand\\\": \\\"TechCorp\\\", \\\"price\\\": 299.99, \\\"launch_date\\\": \\\"2025-01-15\\\"}\",\n"
                + "  \"features\": \"{\\\"color\\\": \\\"black\\\", \\\"size\\\": \\\"42mm\\\", \\\"warranty_years\\\": 2, \\\"waterproof\\\": true}\"\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        assertEquals("PROD-JSON-001", struct.get("product_id"));
        assertEquals("Smart Watch", struct.get("product_name"));

        // Verify JSON_VALUE extractions from metadata
        assertEquals("electronics", struct.get("category"));
        assertEquals("TechCorp", struct.get("brand"));
        assertEquals("299.99", struct.get("price"));

        // Verify JSON_VALUE extractions from features
        assertEquals("black", struct.get("color"));
        assertEquals("42mm", struct.get("size"));
        assertEquals("2", struct.get("warranty_years"));

        // Original JSON strings should still be available
        String metadata = (String) struct.get("metadata");
        assertTrue(metadata.contains("category") && metadata.contains("electronics"));

        String features = (String) struct.get("features");
        assertTrue(features.contains("color") && features.contains("black"));
    }

    @Test
    public void testDoubleFlattenCrossJoinAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"OrderWithMultipleArrays\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"order_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"products\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"shipping_options\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT o.order_id, o.customer_name, " +
                        "p.product_id, p.product_name, p.price, " +
                        "s.\"method\", s.cost, s.delivery_days, " +
                        "(p.price + s.cost) as total_cost " +
                        "FROM inputrecord o " +
                        "JOIN \"inputrecord.products\" p ON TRUE " +
                        "JOIN \"inputrecord.shipping_options\" s ON TRUE");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"order_id\": \"ORD-CROSS-001\",\n"
                + "  \"customer_name\": \"Alice Johnson\",\n"
                + "  \"products\": [\n"
                + "    {\"product_id\": \"PROD-A\", \"product_name\": \"Laptop\", \"price\": 999.99},\n"
                + "    {\"product_id\": \"PROD-B\", \"product_name\": \"Mouse\", \"price\": 29.99},\n"
                + "    {\"product_id\": \"PROD-C\", \"product_name\": \"Keyboard\", \"price\": 79.99}\n"
                + "  ],\n"
                + "  \"shipping_options\": [\n"
                + "    {\"method\": \"Standard\", \"cost\": 5.99, \"delivery_days\": 5},\n"
                + "    {\"method\": \"Express\", \"cost\": 12.99, \"delivery_days\": 2},\n"
                + "    {\"method\": \"Overnight\", \"cost\": 25.99, \"delivery_days\": 1}\n"
                + "  ]\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        // The cross join should return the first combination (first product with first shipping option)
        assertEquals("ORD-CROSS-001", struct.get("order_id"));
        assertEquals("Alice Johnson", struct.get("customer_name"));

        // First product details
        assertNotNull(struct.get("product_id"));
        assertNotNull(struct.get("product_name"));
        assertNotNull(struct.get("price"));

        // First shipping option details
        assertNotNull(struct.get("method"));
        assertNotNull(struct.get("cost"));
        assertNotNull(struct.get("delivery_days"));

        // Calculated total cost (product price + shipping cost)
        assertNotNull(struct.get("total_cost"));

        // Verify the calculation is correct for the first combination
        Double price = (Double) struct.get("price");
        Double shippingCost = (Double) struct.get("cost");
        Double totalCost = (Double) struct.get("total_cost");
        assertEquals(price + shippingCost, totalCost, 0.01);

        // Log the result to see what combination we got
        System.out.println("Cross join result:");
        System.out.println("Product: " + struct.get("product_name") + " ($" + struct.get("price") + ")");
        System.out.println("Shipping: " + struct.get("method") + " ($" + struct.get("cost") + ", " + struct.get("delivery_days") + " days)");
        System.out.println("Total Cost: $" + struct.get("total_cost"));
    }

    @Test
    public void testTripleFlattenWithCalculationsAvroSchema() {
        CSqlTransform<SinkRecord> transform = new CSqlTransform<>();
        Map<String, Object> configs = new HashMap<>();

        String avroSchema = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"ComplexOrderData\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"order_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"customer_tier\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"items\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"discounts\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
                "    {\"name\": \"payment_methods\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
                "  ]\n" +
                "}";

        configs.put(CSqlTransform.STATEMENT_CONFIG,
                "SELECT o.order_id, o.customer_tier, " +
                        "i.item_name, i.quantity, i.unit_price, " +
                        "d.discount_type, d.discount_rate, " +
                        "p.method_name, p.processing_fee, " +
                        "(i.quantity * i.unit_price) as subtotal, " +
                        "(i.quantity * i.unit_price * (1 - d.discount_rate)) as discounted_amount, " +
                        "((i.quantity * i.unit_price * (1 - d.discount_rate)) + p.processing_fee) as final_total " +
                        "FROM inputrecord o " +
                        "JOIN \"inputrecord.items\" i ON TRUE " +
                        "JOIN \"inputrecord.discounts\" d ON TRUE " +
                        "JOIN \"inputrecord.payment_methods\" p ON TRUE");
        configs.put(CSqlTransform.AVRO_SCHEMA_CONFIG, avroSchema);
        transform.configure(configs);

        String inputJson = "{\n"
                + "  \"order_id\": \"ORD-COMPLEX-001\",\n"
                + "  \"customer_tier\": \"GOLD\",\n"
                + "  \"items\": [\n"
                + "    {\"item_name\": \"Premium Headphones\", \"quantity\": 2, \"unit_price\": 199.99},\n"
                + "    {\"item_name\": \"USB Cable\", \"quantity\": 3, \"unit_price\": 15.99}\n"
                + "  ],\n"
                + "  \"discounts\": [\n"
                + "    {\"discount_type\": \"Member\", \"discount_rate\": 0.10},\n"
                + "    {\"discount_type\": \"Seasonal\", \"discount_rate\": 0.15}\n"
                + "  ],\n"
                + "  \"payment_methods\": [\n"
                + "    {\"method_name\": \"Credit Card\", \"processing_fee\": 2.99},\n"
                + "    {\"method_name\": \"PayPal\", \"processing_fee\": 3.49}\n"
                + "  ]\n"
                + "}";

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, inputJson, 0);
        SinkRecord output = transform.apply(record);
        assertNotNull(output);
        Struct struct = (Struct) output.value();

        // The triple cross join should return the first combination
        assertEquals("ORD-COMPLEX-001", struct.get("order_id"));
        assertEquals("GOLD", struct.get("customer_tier"));

        // Verify all components are present
        assertNotNull(struct.get("item_name"));
        assertNotNull(struct.get("quantity"));
        assertNotNull(struct.get("unit_price"));
        assertNotNull(struct.get("discount_type"));
        assertNotNull(struct.get("discount_rate"));
        assertNotNull(struct.get("method_name"));
        assertNotNull(struct.get("processing_fee"));

        // Verify calculated fields
        assertNotNull(struct.get("subtotal"));
        assertNotNull(struct.get("discounted_amount"));
        assertNotNull(struct.get("final_total"));

        // Verify the calculations are mathematically correct
        Integer quantity = (Integer) struct.get("quantity");
        Double unitPrice = (Double) struct.get("unit_price");
        Double discountRate = (Double) struct.get("discount_rate");
        Double processingFee = (Double) struct.get("processing_fee");

        Double expectedSubtotal = quantity * unitPrice;
        Double expectedDiscountedAmount = expectedSubtotal * (1 - discountRate);
        Double expectedFinalTotal = expectedDiscountedAmount + processingFee;

        assertEquals(expectedSubtotal, (Double) struct.get("subtotal"), 0.01);
        assertEquals(expectedDiscountedAmount, (Double) struct.get("discounted_amount"), 0.01);
        assertEquals(expectedFinalTotal, (Double) struct.get("final_total"), 0.01);

        // Log the complex calculation result
        System.out.println("Triple join calculation result:");
        System.out.println("Item: " + struct.get("item_name") + " (Qty: " + quantity + " @ $" + unitPrice + ")");
        System.out.println("Discount: " + struct.get("discount_type") + " (" + (discountRate * 100) + "%)");
        System.out.println("Payment: " + struct.get("method_name") + " (Fee: $" + processingFee + ")");
        System.out.println("Subtotal: $" + struct.get("subtotal"));
        System.out.println("After Discount: $" + struct.get("discounted_amount"));
        System.out.println("Final Total: $" + struct.get("final_total"));
    }
}
