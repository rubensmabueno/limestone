package com.limestone.adapter.parquet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

public class ParquetFilterRule extends RelOptRule {
    public ParquetFilterRule() {
        super(operand(LogicalFilter.class, operand(ParquetTableScan.class, none())),
                "ParquetFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        ParquetTableScan tableScan = call.rel(1);

        call.transformTo(convert(filter, tableScan));

    }

    public RelNode convert(LogicalFilter filter, ParquetTableScan scan) {
        final RelTraitSet traitSet = filter.getTraitSet().replace(ParquetRel.CONVENTION);

        return new ParquetFilter(
                filter.getCluster(),
                traitSet,
                convert(filter.getInput(), ParquetRel.CONVENTION),
                filter.getCondition());
    }
}
