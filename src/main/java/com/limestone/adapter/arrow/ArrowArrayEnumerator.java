package com.limestone.adapter.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class ArrowArrayEnumerator<E> implements Enumerator<List<E>> {
    private final List<VectorSchemaRoot> vectorSchemaRoots;
    private final List<RelDataTypeField> fieldTypes;
    private final AtomicBoolean cancel;

    private int index;
    private int currentPos;

    ArrowArrayEnumerator(List<VectorSchemaRoot> vectorSchemaRoots, AtomicBoolean cancel, RelProtoDataType protoRowType) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        this.vectorSchemaRoots = vectorSchemaRoots;

        this.cancel = cancel;
        this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();

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

        for(RelDataTypeField field : this.fieldTypes) {
            fieldValues.set(field.getIndex(), this.getObject(field.getIndex()));
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
