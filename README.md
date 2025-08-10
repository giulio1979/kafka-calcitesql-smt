# Kafka Calcite SQL SMT

This project implements a Kafka Connect Single Message Transform (SMT) in Java, enabling SQL-based transformation of Kafka records using Apache Calcite.

## Features
- Converts input records (schemaless JSON, Map, or Struct) to a JSON format.
- Executes a SQL SELECT statement (provided via configuration) on each record using Calcite.
- Produces a new Kafka record with a schema based on the SQL result.
- Handles missing fields gracefully (sets them to null).
- Comprehensive unit tests for various input scenarios.

## Example
### Input Record
```
{"a": 1, "b": 2, "c": "{\"json\": 5}", "arr": [1,2,3]}
```

### Configuration
```
kafka.connect.transform.csql.statement: "select a, b from inputrecord"
```

### Output Record
A Kafka record with schema containing fields `a` and `b`.

## Supported Input Types
- Schemaless JSON (String)
- Map<String, Object>
- Kafka Connect Struct

## SQL Support
- Basic SELECT statements (e.g., `select a, b from inputrecord`)
- Fields not present in the input are set to null in the output


## Test Scenarios & Examples
### 7. Join with nested array subtable
**Input:**
```
{
	"invoicenumber": 2,
	"lineitems": [
		{ "product": "tomatoes", "price": "3" }
	]
}
```
**SQL:**
```
select a.*, b.*
	from inputrecord a
	join "inputrecord.lineitems" b on true
```
**Expected Output:**
Kafka record with fields 'invoicenumber', 'product', 'price'.

### 8. Join with stringified JSON array subtable
**Input:**
```
{
	"invoicenumber": 2,
	"lineitems": "[ {\"product\": \"tomatoes\", \"price\": \"3\" } ]"
}
```
**SQL:**
```
select a.*, b.*
	from inputrecord a
	join "inputrecord.lineitems" b on true
```
**Expected Output:**
Kafka record with fields 'invoicenumber', 'product', 'price'.

### 9. Join with custom array construction
**Input:**
```
{
	"invoicenumber": 2,
	"lineitems": [ { "product": "tomatoes", "price": "3" } ]
}
```
**SQL:**
```
select
	a.invoicenumber,
	b.product,
	ARRAY[ROW(b.price, 'usd')] as prices
from inputrecord a
	join "inputrecord.lineitems" b on true
```
**Expected Output:**
Kafka record with fields 'invoicenumber', 'product', and 'prices' (array of price and currency).

### 1. Schemaless input with nested JSON field
**Input:** `{"a": 1, "b":2, "c": "{\"json\": 5}", "arr": [1,2,3]}`
**SQL:** `select a, c from inputrecord`
**Expected Output:** Kafka record with schema containing fields 'a' and 'c', where 'c' is parsed as a nested object (string representation).

### 2. Input with array field
**Input:** `{"a": 1, "arr": [1,2,3]}`
**SQL:** `select arr from inputrecord`
**Expected Output:** Kafka record with schema containing field 'arr' as an array type.

### 3. Struct input with missing field in SQL
**Input:** Struct with fields 'a', 'b'
**SQL:** `select a, x from inputrecord`
**Expected Output:** Field 'a' is set, field 'x' is null in output.

### 4. Input with type mismatch
**Input:** `{"a": "string", "b": 2}`
**SQL:** `select a, b from inputrecord`
**Expected Output:** Kafka record with schema, field 'a' as string, 'b' as int.

### 5. Input with null values
**Input:** `{"a": null, "b": 2}`
**SQL:** `select a, b from inputrecord`
**Expected Output:** Kafka record with schema, field 'a' is null.

### 6. Schemaless input with complex SQL (aggregation, function)
**Input:** `{"a": 1, "b": 2}`
**SQL:** `select a + b as sum from inputrecord`
**Expected Output:** Kafka record with schema, field 'sum' = 3.

## Building & Testing
Run the following command to build and test:
```
mvn clean package
```

## Extending
- To support more complex SQL (expressions, functions, aggregations), extend the SQL parsing and execution logic in `CSqlTransform.java`.
- For advanced schema handling, update the schema inference logic.

## License
MIT
