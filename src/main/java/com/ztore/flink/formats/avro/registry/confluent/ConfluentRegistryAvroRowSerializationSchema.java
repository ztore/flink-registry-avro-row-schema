package com.ztore.flink.formats.avro.registry.confluent;

import com.ztore.flink.formats.avro.RegistryAvroRowSerializationSchema;
import com.ztore.flink.formats.avro.SchemaPublisher;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;

public class ConfluentRegistryAvroRowSerializationSchema extends RegistryAvroRowSerializationSchema {

    private static final long serialVersionUID = 35174351793679353L;

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private ConfluentRegistryAvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz,
                                                        SchemaPublisher.SchemaPublisherProvider schemaPublisherProvider) {
        super(recordClazz, schemaPublisherProvider);
    }

    public ConfluentRegistryAvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz, String url, String subject,
                                                       int identityMapCapacity) {
        this(recordClazz, new CachedSchemaPublisherProvider(url, subject, identityMapCapacity));
    }

    public ConfluentRegistryAvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz, String url, String subject) {
        this(recordClazz, new CachedSchemaPublisherProvider(url, subject, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    private ConfluentRegistryAvroRowSerializationSchema(String avroSchemaString,
                                                        SchemaPublisher.SchemaPublisherProvider schemaPublisherProvider) {
        super(avroSchemaString, schemaPublisherProvider);
    }

    public ConfluentRegistryAvroRowSerializationSchema(String avroSchemaString, String url, String subject, int identityMapCapacity) {
        this(avroSchemaString, new CachedSchemaPublisherProvider(url, subject, identityMapCapacity));
    }

    public ConfluentRegistryAvroRowSerializationSchema(String avroSchemaString, String url, String subject) {
        this(avroSchemaString, new CachedSchemaPublisherProvider(url, subject, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    private static class CachedSchemaPublisherProvider implements SchemaPublisher.SchemaPublisherProvider {

        private static final long serialVersionUID = 8701436325871250L;
        private final String url;
        private final String subject;
        private final int identityMapCapacity;

        CachedSchemaPublisherProvider(String url, String subject, int identityMapCapacity) {
            this.url = url;
            this.subject = subject;
            this.identityMapCapacity = identityMapCapacity;
        }

        @Override
        public SchemaPublisher get() {
            return new ConfluentSchemaRegistryPublisher(
                    new CachedSchemaRegistryClient(url, identityMapCapacity),
                    subject
            );
        }
    }
}
