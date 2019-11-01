package com.ztore.flink.formats.avro.registry.confluent;

import com.ztore.flink.formats.avro.RegistryAvroRowDeserializationSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;

public class ConfluentRegistryAvroRowDeserializationSchema extends RegistryAvroRowDeserializationSchema {

    private static final long serialVersionUID = 35174351793679353L;

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private ConfluentRegistryAvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz,
                                                          SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, schemaCoderProvider);
    }

    public ConfluentRegistryAvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz, String url,
                                                         int identityMapCapacity) {
        this(recordClazz, new CachedSchemaCoderProvider(url, identityMapCapacity));
    }

    public ConfluentRegistryAvroRowDeserializationSchema(Class<? extends SpecificRecord> recordClazz, String url) {
        this(recordClazz, new CachedSchemaCoderProvider(url, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    private ConfluentRegistryAvroRowDeserializationSchema(String avroSchemaString,
                                                          SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(avroSchemaString, schemaCoderProvider);
    }

    public ConfluentRegistryAvroRowDeserializationSchema(String avroSchemaString, String url, int identityMapCapacity) {
        this(avroSchemaString, new CachedSchemaCoderProvider(url, identityMapCapacity));
    }

    public ConfluentRegistryAvroRowDeserializationSchema(String avroSchemaString, String url) {
        this(avroSchemaString, new CachedSchemaCoderProvider(url, DEFAULT_IDENTITY_MAP_CAPACITY));
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
