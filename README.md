# Flink Registry Avro Schema for Table API

Avro Format schema that supports schema registry. 

Current implementation includes:

* Deserialization and serialization schema
* Base schema interface with Confluent Schema Registry implementation
* Table descriptor interface
* "Derive Table Schema" feature to derive table schema automatically when deserializaing
* "From Table Schema" feature to derive avro schema automatically when serializaing

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