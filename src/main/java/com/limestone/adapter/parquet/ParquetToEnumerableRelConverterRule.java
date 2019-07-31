package com.limestone.adapter.parquet;

import com.google.common.base.Predicates;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;

public class ParquetToEnumerableRelConverterRule extends ConverterRule {
    public ParquetToEnumerableRelConverterRule() {
        super(RelNode.class, Predicates.<RelNode>alwaysTrue(),
                ParquetRel.CONVENTION, EnumerableConvention.INSTANCE,
                RelFactories.LOGICAL_BUILDER, "ParquetToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new ParquetToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
