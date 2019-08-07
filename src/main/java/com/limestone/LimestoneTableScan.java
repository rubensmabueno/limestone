package com.limestone;

import com.limestone.adapter.arrow.ArrowTable;
import com.limestone.adapter.parquet.ParquetTable;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

class LimestoneTableScan extends TableScan implements EnumerableRel {
    private final RelDataType projectRowType;

    private final ParquetTable parquetTable;
    private final ArrowTable arrowTable;
    private final int[] fields;

    public LimestoneTableScan(RelOptCluster cluster, RelOptTable table, ParquetTable parquetTable, ArrowTable arrowTable, RelDataType projectRowType, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);

        this.parquetTable = parquetTable;
        this.arrowTable = arrowTable;
        this.projectRowType = projectRowType;
        this.fields = fields;
    }

    public LimestoneTableScan(RelOptCluster cluster, RelOptTable table, ParquetTable parquetTable, ArrowTable arrowTable, RelDataType projectRowType) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);

        this.parquetTable = parquetTable;
        this.arrowTable = arrowTable;
        this.projectRowType = projectRowType;

        List<RelDataTypeField> fieldList = projectRowType.getFieldList();

        this.fields = new int[fieldList.size()];

        for(int i = 0; i < fieldList.size(); i++) {
            fields[i] = fieldList.get(i).getIndex();
        }
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LimestoneTableScan(getCluster(), table, parquetTable, arrowTable, this.projectRowType, this.fields);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields));
    }

    @Override public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        for (int field : this.fields) {
            builder.add(fieldList.get(field));
        }

        return builder.build();
    }

    @Override public void register(RelOptPlanner planner) {
        planner.addRule(LimestoneProjectTableScanRule.INSTANCE);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(table.getExpression(LimestoneTable.class),
                                "project", implementor.getRootExpression(), Expressions.constant(fields)))
        );
    }

    public ParquetTable getParquetTable() { return this.parquetTable; }

    public ArrowTable getArrowTable() { return this.arrowTable; }
}
