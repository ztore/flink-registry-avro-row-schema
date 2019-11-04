package com.ztore.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * Validator for {@link ConfluentRegistryAvro}.
 */
public class ConfluentRegistryAvroValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "confluent-registry-avro";
    public static final String FORMAT_RECORD_CLASS = "format.record-class";
    public static final String FORMAT_AVRO_SCHEMA = "format.avro-schema";
    public static final String FORMAT_TOPIC = "format.topic";
    public static final String FORMAT_REGISTRY_URL = "format.registry-url";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
        final boolean deriveFromSchema = properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(false);
        final boolean hasRecordClass = properties.containsKey(FORMAT_RECORD_CLASS);
        final boolean hasSchemaString = properties.containsKey(FORMAT_AVRO_SCHEMA);
        if (deriveFromSchema && (hasRecordClass || hasSchemaString)) {
            throw new ValidationException(
                    "Format cannot define a schema and derive from the table's schema at the same time.");
        } else if (!deriveFromSchema && hasRecordClass && hasSchemaString) {
            throw new ValidationException("A definition of both a schema and JSON schema is not allowed.");
        } else if (!deriveFromSchema && !hasRecordClass && !hasSchemaString) {
            throw new ValidationException("A definition of a schema or JSON schema is required.");
        } else if (hasRecordClass) {
            properties.validateType(FORMAT_RECORD_CLASS, false, false);
        } else if (hasSchemaString) {
            properties.validateString(FORMAT_AVRO_SCHEMA, false, 1);
        }
    }
}
