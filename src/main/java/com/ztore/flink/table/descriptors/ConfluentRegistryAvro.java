package com.ztore.flink.table.descriptors;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.Map;

public class ConfluentRegistryAvro extends FormatDescriptor {

    private Class<? extends SpecificRecord> recordClass;
    private String avroSchema;

    /**
     * Format descriptor for Apache Avro records with Confluent Schema registry.
     */
    public ConfluentRegistryAvro() {
        super(ConfluentRegistryAvroValidator.FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the class of the Avro specific record.
     *
     * @param recordClass class of the Avro record.
     */
    public ConfluentRegistryAvro recordClass(Class<? extends SpecificRecord> recordClass) {
        Preconditions.checkNotNull(recordClass);
        this.recordClass = recordClass;
        return this;
    }

    /**
     * Sets the Avro schema for specific or generic Avro records.
     *
     * @param avroSchema Avro schema string
     */
    public ConfluentRegistryAvro avroSchema(String avroSchema) {
        Preconditions.checkNotNull(avroSchema);
        this.avroSchema = avroSchema;
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        if (null != recordClass) {
            properties.putClass(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS, recordClass);
        }
        if (null != avroSchema) {
            properties.putString(ConfluentRegistryAvroValidator.FORMAT_AVRO_SCHEMA, avroSchema);
        }

        return properties.asMap();
    }
}