package com.ztore.flink.formats.avro.registry.confluent;

import java.io.*;

public class SerializationTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException  {
//        ConfluentRegistryAvroRowSerializationSchema schema = new ConfluentRegistryAvroRowSerializationSchema(
//            "{" +
//                    "  \"namespace\": \"org.myorganization\"," +
//                    "  \"type\": \"record\"," +
//                    "  \"name\": \"UserMessage\"," +
//                    "    \"fields\": [" +
//                    "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
//                    "      {\"name\": \"user\", \"type\": \"long\"}," +
//                    "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
//                    "    ]" +
//                    "}",
//                "http://localhost:8081");
        ConfluentRegistryAvroRowDeserializationSchema schema = new ConfluentRegistryAvroRowDeserializationSchema(
                "{" +
                        "  \"namespace\": \"org.myorganization\"," +
                        "  \"type\": \"record\"," +
                        "  \"name\": \"UserMessage\"," +
                        "    \"fields\": [" +
                        "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
                        "      {\"name\": \"user\", \"type\": \"long\"}," +
                        "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
                        "    ]" +
                        "}",
                "http://localhost:8081");

        serde(schema);

    }

    // Serialization code
    static <T extends Serializable> void serialize(T obj) throws IOException {
        try (FileOutputStream fos = new FileOutputStream("data.obj");
             ObjectOutputStream oos = new ObjectOutputStream(fos))
        {
            oos.writeObject(obj);
        }
    }

    // Deserialization code
    static <T extends Serializable> T deserialize() throws IOException, ClassNotFoundException {
        try (FileInputStream fis = new FileInputStream("data.obj");
             ObjectInputStream ois = new ObjectInputStream(fis))
        {
            return (T) ois.readObject();
        }
    }

    static <T extends Serializable> T serde(T obj) throws IOException, ClassNotFoundException {
        System.out.println("Object before serialization  => " + obj.toString());
        serialize(obj);
        T newObj = deserialize();
        System.out.println("Object after serialization  => " + newObj.toString());
        return newObj;
    }
}
