package com.limestone.adapter.arrow;

import org.apache.arrow.vector.*;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public abstract class ArrowTable extends AbstractTable {
  protected final List<VectorSchemaRoot> vectorSchemaRoots;
  protected final RelProtoDataType protoRowType;

  ArrowTable(List<VectorSchemaRoot> vectorSchemaRoots, RelProtoDataType protoRowType) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.protoRowType = protoRowType;
  }

  public List<VectorSchemaRoot> getVectorSchemaRoots() { return this.vectorSchemaRoots; }
}
