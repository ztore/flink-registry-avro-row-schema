package com.ztore.flink.formats.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;

public class ConfluentRegistryAvroRowDeserializationSchema<T extends  SpecificRecord> extends RegistryAvroRowDeserializationSchema {

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private ConfluentRegistryAvroRowDeserializationSchema(Class<T> recordClazz,
                                                          SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, schemaCoderProvider);
    }

    public ConfluentRegistryAvroRowDeserializationSchema(Class<T> recordClazz, String url, int identityMapCapacity) {
        this(recordClazz, new CachedSchemaCoderProvider(url, identityMapCapacity));
    }

    public ConfluentRegistryAvroRowDeserializationSchema(Class<T> recordClazz, String url) {
        this(recordClazz, new CachedSchemaCoderProvider(url, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    private static class CachedSchemaCoderProvider implements SchemaCoder.SchemaCoderProvider {

        private static final long serialVersionUID = 213584805303644L;
        private final String url;
        private final int identityMapCapacity;

        CachedSchemaCoderProvider(String url, int identityMapCapacity) {
            this.url = url;
            this.identityMapCapacity = identityMapCapacity;
        }

        @Override
        public SchemaCoder get() {
            return new ConfluentSchemaRegistryCoder(new CachedSchemaRegistryClient(
                    url,
                    identityMapCapacity));
        }
    }
}
