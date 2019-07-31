package com.limestone.adapter.parquet;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParquetTable extends AbstractQueryableTable implements TranslatableTable {
    private final File file;
    private final ParquetMetadata parquetMetadata;

    public ParquetTable(File rootDir) throws IOException {
        super(Object[].class);

        this.file = rootDir;
        this.parquetMetadata = ParquetFileReader.readFooter(new Configuration(), new Path(file.toURI()), ParquetMetadataConverter.NO_FILTER);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return ParquetEnumerator.deduceRowType(typeFactory, parquetMetadata);
    }

    public Enumerable<Object> runQuery(final List<String> fields, final String predicate) {
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType(typeFactory);

        if (fields.isEmpty()) {
            for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
                fieldInfo.add(relDataTypeField);
            }
        } else {
            for (String field : fields) {
                fieldInfo.add(rowType.getField(field, true, false));
            }
        }
        final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return new ParquetEnumerator(file, new AtomicBoolean(false), resultRowType, predicate);
            }
        };
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        List<String> fieldNames = relOptTable.getRowType().getFieldNames();
        return new ParquetTableScan(context.getCluster(), context.getCluster().traitSetOf(ParquetRel.CONVENTION),
                relOptTable, this, relOptTable.getRowType());
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }
}
