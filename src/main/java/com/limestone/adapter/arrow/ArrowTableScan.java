package com.limestone.adapter.arrow;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

class ArrowTableScan extends TableScan implements EnumerableRel {
    final ArrowTranslatableTable arrowTable;
    final int[] fields;

    protected ArrowTableScan(RelOptCluster cluster, RelOptTable table, ArrowTranslatableTable arrowTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);

        this.arrowTable = arrowTable;
        this.fields = fields;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ArrowTableScan(getCluster(), table, arrowTable, fields);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields));
    }

    @Override public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        for (int field : fields) {
            builder.add(fieldList.get(field));
        }

        return builder.build();
    }

    @Override public void register(RelOptPlanner planner) {
        planner.addRule(ArrowProjectTableScanRule.INSTANCE);
    }

    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(table.getExpression(ArrowTranslatableTable.class),
                                "project", implementor.getRootExpression(),
                                Expressions.constant(fields))));
    }
}
