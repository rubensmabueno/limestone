package com.limestone.adapter.parquet;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParquetEnumerator<E> implements Enumerator<E> {
    private final ParquetReader<SimpleRecord> reader;
    private SimpleRecord current;
    private final AtomicBoolean cancel;
    private final List<RelDataTypeField> fieldTypes;

    public ParquetEnumerator(File fileToRead, AtomicBoolean cancel, RelProtoDataType protoRowType, String predicate) {
        this.cancel = cancel;
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();

        try {
            this.reader = ParquetReader.builder(new SimpleReadSupport(), new Path(fileToRead.toURI())).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static RelDataType deduceRowType(RelDataTypeFactory typeFactory, ParquetMetadata parquetMetadata) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        MessageType schema = parquetMetadata.getFileMetaData().getSchema();
        List<Type> fields = schema.getFields();

        for(Type field : fields) {
            RelDataType relDataType = ParquetFieldType.of(field.asPrimitiveType().getPrimitiveTypeName()).toType(typeFactory);

            names.add(field.getName().toUpperCase());
            types.add(relDataType);
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public void reset() {}

    @Override public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public boolean moveNext() {
        if (cancel.get()) {
            return false;
        }

        try {
            current = this.reader.read();

            if (current == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override public E current() {
        if (fieldTypes.size() == 1) {
            Object row = null;

            List<SimpleRecord.NameValue> values = current.getValues();

            String name = fieldTypes.get(0).getName();
            RelDataType type = fieldTypes.get(0).getType();

            for (SimpleRecord.NameValue value : values) {
                if (value.getName().toUpperCase().equals(name.toUpperCase())) {
                    row = convertRow(type, value);
                }
            }

            return (E) row;
        } else {
            Object[] row = new Object[fieldTypes.size()];

            List<SimpleRecord.NameValue> values = current.getValues();

            int i = 0;

            for (RelDataTypeField fieldType : fieldTypes) {

                String name = fieldType.getName();
                RelDataType type = fieldType.getType();

                Boolean entered = false;

                for (SimpleRecord.NameValue value : values) {
                    if (value.getName().toUpperCase().equals(name.toUpperCase())) {
                        entered = true;

                        row[i] = convertRow(type, value);
                        i++;
                    }
                }

                if(!entered) {
                    row[i] = null;
                    i++;
                }
            }

            return (E) row;
        }
    }

    private E convertRow(RelDataType type, SimpleRecord.NameValue row) {
        switch(type.getSqlTypeName()) {
            case DOUBLE:
                return (E) row.getValue();
            case BIGINT:
                return (E) (Object) BytesUtils.bytesToLong((byte[]) row.getValue());
            default:
                return (E) row.getValue();
        }
    }
}


