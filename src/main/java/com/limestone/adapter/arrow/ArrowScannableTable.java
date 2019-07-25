package com.limestone.adapter.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;

import java.util.List;

class ArrowScannableTable extends ArrowTable implements ScannableTable {
  public ArrowScannableTable(List<VectorSchemaRoot> vectorSchemaRoots, RelProtoDataType protoRowType) {
    super(vectorSchemaRoots, protoRowType);
  }

  public String toString() {
    return "ArrowScannableTable";
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new ArrowArrayEnumerator(vectorSchemaRoots);
      }
    };
  }
}
