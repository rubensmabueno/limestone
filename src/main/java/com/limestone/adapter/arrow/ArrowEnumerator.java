package com.limestone.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ArrowEnumerator<E> implements Enumerator<E> {
    private final List<VectorSchemaRoot> vectorSchemaRoots;
    private final List<RelDataTypeField> fieldTypes;
    private final AtomicBoolean cancel;
    private final RelProtoDataType protoRowType;

    private int index;
    private int currentPos;

    public ArrowEnumerator(List<VectorSchemaRoot> vectorSchemaRoots, AtomicBoolean cancel, RelProtoDataType protoRowType) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        this.vectorSchemaRoots = vectorSchemaRoots;

        this.cancel = cancel;
        this.protoRowType = protoRowType;
        this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();

        this.index = 0;
        this.currentPos= 0;
    }

    public static RelDataType deduceRowType(RelDataTypeFactory typeFactory, List<VectorSchemaRoot> vectorSchemaRoots) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        for(Field field : vectorSchemaRoots.get(0).getSchema().getFields()) {
            RelDataType relDataType = ArrowFieldType.of(field.getType()).toType((JavaTypeFactory) typeFactory);

            names.add(field.getName().toUpperCase());
            types.add(relDataType);
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public void close() {}

    @Override public void reset() {
        this.currentPos = 0;
    }

    @Override public boolean moveNext() {
        if (this.currentPos < (this.vectorSchemaRoots.get(this.index).getRowCount() - 1)) {
            this.currentPos += 1;
            return true;
        } else if (this.index < (this.vectorSchemaRoots.size() - 1)) {
            this.index += 1;
            this.currentPos = 0;
            return true;
        }

        return false;
    }

    @Override public E current() {
        if (fieldTypes.size() == 1) {
            return getObject(fieldTypes.get(0));
        } else {
            Object[] fieldValues = new Object[fieldTypes.size() + 1];

            for(RelDataTypeField field : this.fieldTypes) {
                fieldValues[field.getIndex()] = this.getObject(field);
            }

            return (E) fieldValues;
        }
    }

    private E getObject(RelDataTypeField relDataTypeField) {
        FieldVector fieldVector = this.vectorSchemaRoots.get(this.index).getVector(relDataTypeField.getName());

        if (fieldVector.getValueCount() <= this.currentPos) {
            return (E) "NULL";
        } else {
            if (relDataTypeField.getType().getFullTypeString().equals("JavaType(class java.lang.Long)")) {
                return (E) new Long((Integer) fieldVector.getObject(this.currentPos));
            } else {
                return (E) fieldVector.getObject(this.currentPos);
            }
        }
    }
}

