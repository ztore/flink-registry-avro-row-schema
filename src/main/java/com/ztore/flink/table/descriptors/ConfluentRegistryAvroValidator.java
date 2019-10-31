package com.ztore.flink.table.descriptors;

import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * Validator for {@link ConfluentRegistryAvro}.
 */
public class ConfluentRegistryAvroValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "confluent-registry-avro";
    public static final String FORMAT_RECORD_CLASS = "format.record-class";
    public static final String FORMAT_AVRO_SCHEMA = "format.avro-schema";
    public static final String FORMAT_REGISTRY_URL = "format.registry-url";
}
