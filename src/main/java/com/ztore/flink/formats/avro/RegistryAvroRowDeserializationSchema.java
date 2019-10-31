package com.ztore.flink.formats.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.types.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

public class RegistryAvroRowDeserializationSchema<T extends  SpecificRecord> implements DeserializationSchema<Row> {

    private static final long serialVersionUID = 61736557687436545L;

    /**
     * Used for time conversions into SQL types.
     */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /** Provider for schema coder. Used for initializing in each task. */
    private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

    /** Coder used for reading schema from incoming stream. */
    private transient SchemaCoder schemaCoder;

    /** Avro record class for deserialization. */
    private Class<T> recordClazz;

    /**
     * Schema string for deserialization.
     */
    private String schemaString;

    /**
     * Avro serialization schema.
     */
    private transient Schema schema;

    /**
     * Type information describing the result type.
     */
    private transient RowTypeInfo typeInfo;

    /**
     * Record to deserialize byte array.
     */
    private transient IndexedRecord record;

    /**
     * Reader that deserializes byte array into a record.
     */
    private transient SpecificDatumReader<IndexedRecord> datumReader;

    /**
     * Input stream to read message from.
     */
    private transient MutableByteArrayInputStream inputStream;

    /**
     * Avro decoder that decodes binary data.
     */
    private transient Decoder decoder;

    /**
     * Creates Avro deserialization schema that reads schema from input stream using provided {@link SchemaCoder}.
     *
     * @param recordClazz         class to which deserialize. Should be {@link SpecificRecord}.
     * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder} that will be used for
     *                            schema reading
     */
    protected RegistryAvroRowDeserializationSchema(Class<T> recordClazz,
                                                 SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        this.recordClazz = recordClazz;
        this.schemaCoderProvider = schemaCoderProvider;
        schemaCoder = schemaCoderProvider.get();
        schema = SpecificData.get().getSchema(recordClazz);
        typeInfo = (RowTypeInfo) AvroSchemaConverter.convertToTypeInfo(recordClazz);
        schemaString = schema.toString();
        record = (IndexedRecord) SpecificData.newInstance(recordClazz, schema);
        datumReader = new SpecificDatumReader<>(schema);
        inputStream = new MutableByteArrayInputStream();
        decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        checkAvroInitialized();
        try {
            getInputStream().setBuffer(message);
            Schema writerSchema = schemaCoder.readSchema(getInputStream());
            SpecificDatumReader<IndexedRecord> reader = getDatumReader();
            reader.setSchema(writerSchema);
            reader.setExpected(schema);
            record = reader.read(record, decoder);
            return convertAvroRecordToRow(schema, typeInfo, record);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Avro record.", e);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RegistryAvroRowDeserializationSchema that = (RegistryAvroRowDeserializationSchema) o;
        return Objects.equals(recordClazz, that.recordClazz) &&
                Objects.equals(schemaString, that.schemaString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordClazz, schemaString);
    }

    SpecificDatumReader<IndexedRecord> getDatumReader() {
        return datumReader;
    }

    MutableByteArrayInputStream getInputStream() {
        return inputStream;
    }

    Decoder getDecoder() {
        return decoder;
    }

    void checkAvroInitialized() {
        if (datumReader == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            SpecificData specificData = new SpecificData(cl);
            this.datumReader = new SpecificDatumReader<>(specificData);
            this.schema = specificData.getSchema(recordClazz);
            this.typeInfo = (RowTypeInfo) AvroSchemaConverter.convertToTypeInfo(recordClazz);
            this.schemaString = schema.toString();
            this.record = (IndexedRecord) SpecificData.newInstance(recordClazz, schema);
            this.inputStream = new MutableByteArrayInputStream();
            this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        }
        if (schemaCoder == null) {
            this.schemaCoder = schemaCoderProvider.get();
        }
    }

    // --------------------------------------------------------------------------------------------

    private Row convertAvroRecordToRow(Schema schema, RowTypeInfo typeInfo, IndexedRecord record) {
        final List<Schema.Field> fields = schema.getFields();
        final TypeInformation<?>[] fieldInfo = typeInfo.getFieldTypes();
        final int length = fields.size();
        final Row row = new Row(length);
        for (int i = 0; i < length; i++) {
            final Schema.Field field = fields.get(i);
            row.setField(i, convertAvroType(field.schema(), fieldInfo[i], record.get(i)));
        }
        return row;
    }

    private Object convertAvroType(Schema schema, TypeInformation<?> info, Object object) {
        // we perform the conversion based on schema information but enriched with pre-computed
        // type information where useful (i.e., for arrays)

        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                if (object instanceof IndexedRecord) {
                    return convertAvroRecordToRow(schema, (RowTypeInfo) info, (IndexedRecord) object);
                }
                throw new IllegalStateException("IndexedRecord expected but was: " + object.getClass());
            case ENUM:
            case STRING:
                return object.toString();
            case ARRAY:
                if (info instanceof BasicArrayTypeInfo) {
                    final TypeInformation<?> elementInfo = ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo();
                    return convertToObjectArray(schema.getElementType(), elementInfo, object);
                } else {
                    final TypeInformation<?> elementInfo = ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo();
                    return convertToObjectArray(schema.getElementType(), elementInfo, object);
                }
            case MAP:
                final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;
                final Map<String, Object> convertedMap = new HashMap<>();
                final Map<?, ?> map = (Map<?, ?>) object;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    convertedMap.put(
                            entry.getKey().toString(),
                            convertAvroType(schema.getValueType(), mapTypeInfo.getValueTypeInfo(), entry.getValue()));
                }
                return convertedMap;
            case UNION:
                final List<Schema> types = schema.getTypes();
                final int size = types.size();
                final Schema actualSchema;
                if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    return convertAvroType(types.get(1), info, object);
                } else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                    return convertAvroType(types.get(0), info, object);
                } else if (size == 1) {
                    return convertAvroType(types.get(0), info, object);
                } else {
                    // generic type
                    return object;
                }
            case FIXED:
                final byte[] fixedBytes = ((GenericFixed) object).bytes();
                if (info == Types.BIG_DEC) {
                    return convertToDecimal(schema, fixedBytes);
                }
                return fixedBytes;
            case BYTES:
                final ByteBuffer byteBuffer = (ByteBuffer) object;
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                if (info == Types.BIG_DEC) {
                    return convertToDecimal(schema, bytes);
                }
                return bytes;
            case INT:
                if (info == Types.SQL_DATE) {
                    return convertToDate(object);
                } else if (info == Types.SQL_TIME) {
                    return convertToTime(object);
                }
                return object;
            case LONG:
                if (info == Types.SQL_TIMESTAMP) {
                    return convertToTimestamp(object);
                }
                return object;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object;
        }
        throw new RuntimeException("Unsupported Avro type:" + schema);
    }

    private BigDecimal convertToDecimal(Schema schema, byte[] bytes) {
        final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        return new BigDecimal(new BigInteger(bytes), decimalType.getScale());
    }

    private Date convertToDate(Object object) {
        final long millis;
        if (object instanceof Integer) {
            final Integer value = (Integer) object;
            // adopted from Apache Calcite
            final long t = (long) value * 86400000L;
            millis = t - (long) LOCAL_TZ.getOffset(t);
        } else {
            // use 'provided' Joda time
            final LocalDate value = (LocalDate) object;
            millis = value.toDate().getTime();
        }
        return new Date(millis);
    }

    private Time convertToTime(Object object) {
        final long millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else {
            // use 'provided' Joda time
            final LocalTime value = (LocalTime) object;
            millis = (long) value.get(DateTimeFieldType.millisOfDay());
        }
        return new Time(millis - LOCAL_TZ.getOffset(millis));
    }

    private Timestamp convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else {
            // use 'provided' Joda time
            final DateTime value = (DateTime) object;
            millis = value.toDate().getTime();
        }
        return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
    }

    private Object[] convertToObjectArray(Schema elementSchema, TypeInformation<?> elementInfo, Object object) {
        final List<?> list = (List<?>) object;
        final Object[] convertedArray = (Object[]) Array.newInstance(
                elementInfo.getTypeClass(),
                list.size());
        for (int i = 0; i < list.size(); i++) {
            convertedArray[i] = convertAvroType(elementSchema, elementInfo, list.get(i));
        }
        return convertedArray;
    }
}

