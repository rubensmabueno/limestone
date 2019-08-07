package com.limestone.adapter.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class ArrowTranslatableTable extends ArrowTable implements QueryableTable, TranslatableTable {
    public ArrowTranslatableTable(List<VectorSchemaRoot> vectorSchemaRoots) {
        super(vectorSchemaRoots, null);
    }

    public String toString() {
        return "ArrowTranslatableTable";
    }

    public Enumerable<Object> project(final DataContext root, final int[] fields) {
        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return new ArrowEnumerator<>(vectorSchemaRoots, new AtomicBoolean(false), protoRowType);
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return ArrowEnumerator.deduceRowType(typeFactory, vectorSchemaRoots);
    }

    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public Type getElementType() {
        return Object[].class;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final int[] fields = IntStream.rangeClosed(0, fieldCount - 1).toArray();

        return new ArrowTableScan(context.getCluster(), relOptTable, this, fields);
    }
}
