package com.limestone.adapter.parquet;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

public class ParquetProjectRule extends ParquetConvertRule  {
    public ParquetProjectRule() {
        super(LogicalProject.class, "ParquetProjectRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace(out);
        return new ParquetProject(project.getCluster(), traitSet,
                convert(project.getInput(), out), project.getProjects(),
                project.getRowType());
    }
}
