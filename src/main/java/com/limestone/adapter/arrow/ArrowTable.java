package com.limestone.adapter.arrow;

import org.apache.arrow.vector.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

abstract class ArrowTable extends AbstractTable {
  public final List<VectorSchemaRoot> vectorSchemaRoots;
  final RelProtoDataType protoRowType;

  ArrowTable(List<VectorSchemaRoot> vectorSchemaRoots, RelProtoDataType protoRowType) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.protoRowType = protoRowType;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }

    return ArrowEnumerator.deduceRowType((JavaTypeFactory) typeFactory, this.vectorSchemaRoots.get(0));
  }
}
