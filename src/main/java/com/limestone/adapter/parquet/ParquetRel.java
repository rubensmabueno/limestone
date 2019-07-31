package com.limestone.adapter.parquet;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public interface ParquetRel extends RelNode {
    void implement(Implementor implementor);

    Convention CONVENTION = new Convention.Impl("PARQUET", ParquetRel.class);

    class Implementor {
        final List<String> projectedFields = new ArrayList<>();
        String filter = "";

        RelOptTable table;
        ParquetTable parquetTable;

        public void addProjectedField(String field) {
            projectedFields.add(field);
        }

        public void setFilter(String filter) {
            this.filter = filter;
        }

        public String getFilter() {
            return filter;
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((ParquetRel) input).implement(this);
        }
    }
}
