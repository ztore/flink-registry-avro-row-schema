package com.ztore.flink.formats.avro;

import com.ztore.flink.formats.avro.typeutils.RowToAvroSchemaConverter;
import com.ztore.flink.table.descriptors.ConfluentRegistryAvroValidator;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of Avro-to-row-with-schema-registry
 * {@link DeserializationSchema}.
 */
public class ConfluentRegistryAvroRowFormatFactory extends TableFormatFactoryBase<Row>
        implements DeserializationSchemaFactory<Row>, SerializationSchemaFactory<Row> {

    public ConfluentRegistryAvroRowFormatFactory() {
        super(ConfluentRegistryAvroValidator.FORMAT_TYPE_VALUE, 1, true);
    }

    @Override
    protected List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS);
        properties.add(ConfluentRegistryAvroValidator.FORMAT_AVRO_SCHEMA);
        properties.add(ConfluentRegistryAvroValidator.FORMAT_AVRO_NAMESPACE);
        properties.add(ConfluentRegistryAvroValidator.FORMAT_AVRO_RECORD_NAME);
        properties.add(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL);
        return properties;
    }

    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        if (descriptorProperties.containsKey(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS)) {
            return new ConfluentRegistryAvroRowDeserializationSchema(
                    descriptorProperties.getClass(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class),
                    descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL)
            );
        } else {
            return new ConfluentRegistryAvroRowDeserializationSchema(
                    descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_AVRO_SCHEMA),
                    descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL)
            );
        }
    }

    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        if (descriptorProperties.containsKey(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS)) {
            return new ConfluentRegistryAvroRowSerializationSchema(
                descriptorProperties.getClass(ConfluentRegistryAvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class),
                descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL)
            );
        } else if (descriptorProperties.containsKey(ConfluentRegistryAvroValidator.FORMAT_AVRO_SCHEMA)) {
            return new ConfluentRegistryAvroRowSerializationSchema(
                descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_AVRO_SCHEMA),
                descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL)
            );
        } else {
            return new ConfluentRegistryAvroRowSerializationSchema(
                RowToAvroSchemaConverter.convertTableSchema(
                        deriveSchema(descriptorProperties.asMap()),
                        descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_AVRO_RECORD_NAME),
                        descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_AVRO_NAMESPACE)
                ).toString(),
                descriptorProperties.getString(ConfluentRegistryAvroValidator.FORMAT_REGISTRY_URL)
            );
        }
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);

        // validate
        new ConfluentRegistryAvroValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
