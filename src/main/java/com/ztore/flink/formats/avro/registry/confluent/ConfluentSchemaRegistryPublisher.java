package com.ztore.flink.formats.avro.registry.confluent;

import com.ztore.flink.formats.avro.SchemaPublisher;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ConfluentSchemaRegistryPublisher implements SchemaPublisher {

    private final SchemaRegistryClient schemaRegistryClient;
    private final String subject;
    private static final byte MAGIC_BYTE = 0x0;
    private static final int idSize = 4;

    /**
     * Creates {@link SchemaPublisher} that uses provided {@link SchemaRegistryClient} to connect to
     * schema registry.
     *
     * @param schemaRegistryClient client to connect schema registry
     */
    public ConfluentSchemaRegistryPublisher(SchemaRegistryClient schemaRegistryClient, String subject) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.subject = subject;
    }

    @Override
    public void writeSchema(Schema schema, ByteArrayOutputStream out) throws RuntimeException {
        try {
            int id = schemaRegistryClient.register(subject, schema);
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(idSize).putInt(id).array());
        } catch (IOException e) {
            throw new RuntimeException("Error serializing Avro message", e);
        } catch (RestClientException e) {
            throw new RuntimeException("Error registering Avro schema: " + schema, e);
        }
    }
}
