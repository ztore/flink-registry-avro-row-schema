package com.ztore.flink.table.descriptors;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.descriptors.Schema;

public class AvroSchema extends Schema {

    public Schema avroSchema(String avroSchema) {
        TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchema);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
        for (int i = 0; i < rowTypeInfo.getTotalFields(); i++) {
            field(rowTypeInfo.getFieldNames()[i], rowTypeInfo.getFieldTypes()[i]);
        }
        return this;
    }

    public Schema recordClass(Class<? extends SpecificRecord> recordClass) {
        TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(recordClass);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
        for (int i = 0; i < rowTypeInfo.getTotalFields(); i++) {
            field(rowTypeInfo.getFieldNames()[i], rowTypeInfo.getFieldTypes()[i]);
        }
        return this;
    }
}
