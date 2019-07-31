package com.limestone.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class ArrowEnumerator<E> implements Enumerator<E> {
    private final List<VectorSchemaRoot> vectorSchemaRoots;
    private final int[] fields;
    private int index;
    private int currentPos;

    ArrowEnumerator(List<VectorSchemaRoot> vectorSchemaRoots, int[] fields) {
        this.vectorSchemaRoots = vectorSchemaRoots;
        this.fields = fields;
        this.index = 0;
        this.currentPos= 0;
    }

    public static RelDataType deduceRowType(JavaTypeFactory typeFactory, VectorSchemaRoot vectorSchemaRoot) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        for(FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            RelDataType relDataType = ArrowFieldType.of(fieldVector.getField().getType()).toType(typeFactory);

            names.add(fieldVector.getField().getName().toUpperCase());
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
        if (fields.length == 1) {
            return getObject(fields[0]);
        } else {
            Object[] fieldValues = new Object[fields.length + 1];

            for(int field : this.fields) {
                fieldValues[field] = this.getObject(field);
            }

            return (E) fieldValues;
        }
    }

    private E getObject(int fieldIndex) {
        FieldVector fieldVector = this.vectorSchemaRoots.get(this.index).getFieldVectors().get(fieldIndex);

        if (fieldVector.getValueCount() <= this.currentPos) {
            return (E) "NULL";
        } else {
            return (E) fieldVector.getObject(this.currentPos);
        }
    }
}

