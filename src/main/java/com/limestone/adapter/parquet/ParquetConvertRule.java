package com.limestone.adapter.parquet;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;

abstract class ParquetConvertRule extends ConverterRule {
    protected final Convention out;

    ParquetConvertRule(Class<? extends RelNode> clazz, String description) {
        this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    <R extends RelNode> ParquetConvertRule(Class<R> clazz, Predicate<? super R> predicate, String description) {
        super(clazz, predicate, Convention.NONE, ParquetRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description);
        this.out = ParquetRel.CONVENTION;
    }
}
