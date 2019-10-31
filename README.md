# Flink Registry Avro Schema for Table API

Avro Format schema that supports schema registry. 

Current implementation includes:

* Deserialization schema only
* Base schema interface with Confluent Schema Registry implementation
* Table descriptor interface
* "Derive Schema" feature to derive table schema automatically

It merges [RegistryAvroDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.html) and [AvroRowDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/AvroRowDeserializationSchema.html) from the official Flink

## Installation

Clone the repository, and install the package with

```
mvn clean install
``` 

## Usage

1. Import the class

    ```scala
    import com.ztore.flink.table.descriptors.ConfluentRegistryAvro
    ```
   
2. Use the class in the withFormat method

    ```scala
   tableEnv
     .connect(
       new Kafka()
     )
     .withFormat(
       new ConfluentRegistryAvro()
         .avroSchema(...)      // avro schema string
         .registryUrl("http://localhost:8081")      // confluent schema registry url
         .deriveSchema()       // use this to automatically include all columns in resulting table
     )
     .inAppendMode()
     .registerTableSource("my-table")
    ```