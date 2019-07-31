package com.limestone.adapter.parquet;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class ParquetFilter extends Filter implements ParquetRel {
    private final String match;

    public ParquetFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() == ParquetRel.CONVENTION;
        assert getConvention() == child.getConvention();

        this.match = condition.toString();
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new ParquetFilter(getCluster(), getTraitSet(), input,getCondition());
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.setFilter(match);
    }
}
