# Flink Registry Avro Schema for Table API

Avro Format schema that supports schema registry. 

Current implementation includes:

* Deserialization and serialization schema
* Base schema interface with Confluent Schema Registry implementation
* Table descriptor interface
* "Derive Table Schema" feature to derive table schema automatically when deserializaing
* "From Table Schema" feature to derive avro schema automatically when serializaing

Essentially, the deserialization schema combines [RegistryAvroDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.html) 
and [AvroRowDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/AvroRowDeserializationSchema.html) from the official Flink. 
The serialization schema is modified from [AvroRowSerializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/AvroRowSerializationSchema.html)

## Features

### Deserialization

The existing official deserialization schemas each has their limitations:

* [RegistryAvroDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.html)
    * It deserialize to a SpecificRecord or a GenericRecord instead of a Flink Row, thus requires an extra for usage in the Table API
    * Therefore, it also lacks the new table connector interface
    
* [AvroRowDeserializationSchema](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/formats/avro/AvroRowDeserializationSchema.html)
    * It does not support a schema registry for handling changing schema, which is especially critical when using database CDC sources such as debezium
    * When using the new table connector interface, it requires definition of table schema separately. 
    
The deserialization schema in this package overcome these limitations with the following features:
* Supports a schema registry for changing writer schema
* Seserializes into Flink Rows, thus integrated to the table API
* Denerates table schema automatically, containing all avro columns, using [AvroSchemaConverter](https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/org/apache/flink/formats/avro/typeutils/AvroSchemaConverter.html)
* Provides a table connector interface through ConfluentRegistryAvro (descriptor need to be developed per registry provider)

### Serialization

The existing serialization schema is not integrated with schema registried. The one in the package supports it with:
* a SchemaPublisher interface, for publishing the data schema to a schema registry
* an interface RegistryAvroRowSerializationSchema, where a schemaPublisher would write information about the schema into the output message.
* the RowToAvroSchemaConverter, which converts a flink table schema into an Avro schema

With these features, serializing a table to avro would be simplified into:
1. setting the table schema in .withSchema
2. providing connection details of the schema registry in .withFormat
3. the deserialization schema will automatically derive an avro schema from the table schema, then publish the schema to the schema regisrtry, as well as writing necessary info in the producced message. 

The .fromTableSchema feature is similar to the .deriveSchema method of the 
[CSV fromat](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/connect.html#csv-format) 
and [JSON format](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/connect.html#json-format).

The converter currently supports the following types only:

Row, Array, Multiset, Map, 
VarChar, Char, Boolean, Bytes, 
Decimal, TinyInt, SmallInt, Int, BigInt, Float, Double,
Time, Timestamp, DayTimeInterval, Null

## Installation

Clone the repository, and install the package with

```
mvn clean install
``` 

## Usage - Confluent Schema Registry

1. Import the class

    ```scala
    import com.ztore.flink.table.descriptors.ConfluentRegistryAvro
    ```
   
2. Use the class in the withFormat method

    ```scala
   // TableSource
   tableEnv
     .connect(
       new Kafka()
     )
     .withFormat(
       new ConfluentRegistryAvro()
   
         // define the schema either by using an Avro specific record class
         .recordClass(classOf[User])
   
         // or by using an Avro schema
         .avroSchema(
             "{" +
             "  \"namespace\": \"org.myorganization\"," +
             "  \"type\": \"record\"," +
             "  \"name\": \"UserMessage\"," +
             "    \"fields\": [" +
             "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
             "      {\"name\": \"user\", \"type\": \"long\"}," +
             "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
             "    ]" +
             "}"
         )      
   
         // required: confluent schema registry url
         .registryUrl("http://localhost:8081")      
   
         // use this to automatically include all columns in resulting table
         .deriveTableSchema()
     )
     .inAppendMode()
     .registerTableSource("my-table")
   
   //TableSink
      tableEnv
        .connect(
          new Kafka()
        )
        .withFormat(
          new ConfluentRegistryAvro()
   
            // define the schema either by using an Avro specific record class
            .recordClass(classOf[User])
       
            // or by using an Avro schema
            .avroSchema(
                 "{" +
                 "  \"namespace\": \"org.myorganization\"," +
                 "  \"type\": \"record\"," +
                 "  \"name\": \"UserMessage\"," +
                 "    \"fields\": [" +
                 "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
                 "      {\"name\": \"user\", \"type\": \"long\"}," +
                 "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
                 "    ]" +
                 "}"
            )          
       
            // or use fromTableSchema to derive from table schema
            .fromTableSchema("org.myorganization", "UserMessage")  
            // if you use fromTableSchema, you can optionally provide the Kafka topic name here
            // for the schema subject and namespace
            // if you do not provide one, it will try to read from the topic property from the Kafka connector
            .topic("my-topic")
       
            // required: confluent schema registry url
            .registryUrl("http://localhost:8081")
        )      
        .withSchema(
          new Schema()
            .field("timestamp", Types.STRING())
            .field("user", Types.LONG())
            .field("name", Types.STRING())
        )
        .inAppendMode()
        .registerTableSink("my-table")
    ```