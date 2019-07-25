package com.limestone.adapter.arrow;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class ArrowProjectTableScanRule extends RelOptRule {
    public static final ArrowProjectTableScanRule INSTANCE = new ArrowProjectTableScanRule(RelFactories.LOGICAL_BUILDER);

    private ArrowProjectTableScanRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalProject.class, operand(ArrowTableScan.class, none())), relBuilderFactory, "ArrowProjectTableScanRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final ArrowTableScan scan = call.rel(1);

        int[] fields = getProjectFields(project.getProjects());

        if (fields == null) {
            return;
        }

        call.transformTo(new ArrowTableScan(scan.getCluster(), scan.getTable(), scan.arrowTable, fields));
    }

    private int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];

        for (int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null;
            }
        }

        return fields;
    }
}
