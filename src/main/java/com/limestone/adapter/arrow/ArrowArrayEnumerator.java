package com.limestone.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.Enumerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

class ArrowArrayEnumerator<E> implements Enumerator<List<E>> {
    private final List<VectorSchemaRoot> vectorSchemaRoots;
    private final int[] fields;
    private int index;
    private int currentPos;

    ArrowArrayEnumerator(List<VectorSchemaRoot> vectorSchemaRoots) {
        this(vectorSchemaRoots, IntStream.rangeClosed(0, vectorSchemaRoots.get(0).getFieldVectors().size() - 1).toArray());
    }

    ArrowArrayEnumerator(List<VectorSchemaRoot> vectorSchemaRoots, int[] fields) {
        this.vectorSchemaRoots = vectorSchemaRoots;
        this.fields = fields;
        this.index = 0;
        this.currentPos= 0;
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

    @Override public List<E> current() {
        List<E> fieldValues = new ArrayList<>();

        for(int field : this.fields) {
            fieldValues.set(field, this.getObject(field));
        }

        return fieldValues;
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
