package com.limestone.adapter.parquet;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.util.BuiltInMethod;
import java.util.List;

public class ParquetToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public ParquetToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, child);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(), rowType,
                        pref.prefer(JavaRowFormat.ARRAY));

        final BlockBuilder list = new BlockBuilder();
        final ParquetRel.Implementor parquetImplementor = new ParquetRel.Implementor();
        parquetImplementor.visitChild(0, getInput());

        final Expression table =
                list.append("table", parquetImplementor.table.getExpression(ParquetTable.class));

        final Expression fields =
                list.append("fields", constantArrayList(parquetImplementor.projectedFields, String.class));

        final Expression predicates =
                list.append("predicates", Expressions.constant(parquetImplementor.getFilter()));

        Expression enumerable =
                list.append("enumerable", Expressions.call(table, "runQuery", fields, predicates));

        list.add(Expressions.return_(null, enumerable));

        return implementor.result(physType, list.toBlock());
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ParquetToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
        return Expressions.call(
                BuiltInMethod.ARRAYS_AS_LIST.method,
                Expressions.newArrayInit(clazz, constantList(values)));
    }

    private static <T> List<Expression> constantList(List<T> values) {
        return Lists.transform(values,
                new Function<T, Expression>() {
                    public Expression apply(T a0) {
                        return Expressions.constant(a0);
                    }
                });
    }
}
