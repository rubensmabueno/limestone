package com.limestone.adapter.parquet;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ParquetProject extends Project implements ParquetRel {
    public ParquetProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
        assert getConvention() == ParquetRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        List<String> fieldNames = getRowType().getFieldNames();
        for (String fieldName : fieldNames) {
            implementor.addProjectedField(fieldName);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new ParquetProject(getCluster(), traitSet, input, projects, rowType);
    }
}
