package com.ztore.flink.formats.avro.typeutils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.LogicalTypes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;

import java.util.stream.Collectors;

public class RowToAvroSchemaConverter {

    private RowToAvroSchemaConverter() {
        // private
    }

    public static Schema convertTableSchema(TableSchema tableSchema, String tableName, String namespace) {
        return convertRecord(
                (RowType) tableSchema.toRowDataType().getLogicalType(),
                tableName,
                namespace);
    }

    private static Schema convertRecord(RowType rowType, String fieldName, String namespace) {
        return Schema.createRecord(fieldName, "", namespace, false,
            rowType.getFields().stream().map(
                    p -> new Schema.Field(p.getName(), convertDataType(p.getType(), namespace))
            ).collect(Collectors.toList()));
    }

    private static Schema convertDecimal(DecimalType decimalType) {
        return LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                .addToSchema(Schema.create(Schema.Type.BYTES));
    }

    private static Schema convertDataType(LogicalType dataType, String namespace) {
        if (dataType instanceof RowType) {
            return convertRecord((RowType) dataType,null, namespace);
        } else if (dataType instanceof ArrayType) {
            return Schema.createArray(convertDataType(((ArrayType) dataType).getElementType(), namespace));
        } else if (dataType instanceof MultisetType) {
            return Schema.createArray(convertDataType(((MultisetType) dataType).getElementType(), namespace));
        } else if (dataType instanceof MapType) {
            return  Schema.createMap(convertDataType(((MapType) dataType).getValueType(), namespace));
        } else if (dataType instanceof VarCharType) {
            return Schema.create(Schema.Type.STRING);
        } else if (dataType instanceof CharType) {
            return Schema.create(Schema.Type.STRING);
        } else if (dataType instanceof BooleanType) {
            return Schema.create(Schema.Type.BOOLEAN);
        } else if (dataType instanceof BinaryType) {
            return Schema.create(Schema.Type.BYTES);
        } else if (dataType instanceof DecimalType) {
            return convertDecimal((DecimalType) dataType);
        } else if (dataType instanceof TinyIntType) {
            return Schema.create(Schema.Type.INT);
        } else if (dataType instanceof SmallIntType) {
            return Schema.create(Schema.Type.INT);
        } else if (dataType instanceof IntType) {
            return Schema.create(Schema.Type.INT);
        } else if (dataType instanceof BigIntType) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (dataType instanceof FloatType) {
            return Schema.create(Schema.Type.FLOAT);
        } else if (dataType instanceof DoubleType) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (dataType instanceof DateType) {
            return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        } else if (dataType instanceof TimeType) {
            return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        } else if (dataType instanceof TimestampType) {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (dataType instanceof DayTimeIntervalType) {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        } else if (dataType instanceof NullType) {
            return Schema.create(Schema.Type.NULL);
        } else {
            throw new RuntimeException("Unsupported type:" + dataType);
        }
    }
}
