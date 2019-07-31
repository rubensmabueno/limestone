package com.limestone.adapter.parquet;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class ParquetTableScan extends TableScan implements ParquetRel {
    private final ParquetTable parquetTable;
    private final RelDataType projectRowType;

    public ParquetTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, ParquetTable parquetTable, RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.parquetTable = parquetTable;
        this.projectRowType = projectRowType;
        assert getConvention() == ParquetRel.CONVENTION;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new ParquetTableScan(getCluster(), traitSet, table, parquetTable, projectRowType);
    }

    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override
    public void register(RelOptPlanner planner) {
       planner.addRule(new ParquetToEnumerableRelConverterRule());
       planner.addRule(new ParquetProjectRule());
       planner.addRule(new ParquetFilterRule());
    }

    @Override
    public void implement(ParquetRel.Implementor implementor) {
        implementor.parquetTable = this.parquetTable;
        implementor.table = table;
    }
}
