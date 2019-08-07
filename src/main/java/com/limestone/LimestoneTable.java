package com.limestone;

import com.limestone.adapter.arrow.ArrowTranslatableTable;
import com.limestone.adapter.parquet.ParquetTable;
import com.limestone.adapter.arrow.ArrowFieldType;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;

public class LimestoneTable extends AbstractQueryableTable implements TranslatableTable {
    private final JSONObject tableSchema;

    private final ArrowTranslatableTable arrowTable;
    private final ParquetTable parquetTable;

    public LimestoneTable(File tableDirectory) throws IOException, ParseException {
        super(Object[].class);

        File parquetFile = new File(tableDirectory.getPath() + "/data.parquet");
        File schemaFile = new File(tableDirectory.getPath() + "/schema.json");

        this.tableSchema = (JSONObject) new JSONParser().parse(new FileReader(schemaFile));

        this.arrowTable = new ArrowTranslatableTable(this.getArrowSchema(this.tableSchema));
        this.parquetTable = new ParquetTable(parquetFile);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return LimestoneEnumerator.deduceRowType(typeFactory, this.tableSchema);
    }

    public Enumerable<Object> project(DataContext root, int[] fields) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType(typeFactory);

        if (fields.length == 0) {
            for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
                fieldInfo.add(relDataTypeField);
            }
        } else {
            List<Field> fieldList = getFieldList(this.tableSchema);

            for (int field : fields) {
                fieldInfo.add(rowType.getField((fieldList.get(field).getName()).toUpperCase(), true, false));
            }
        }

        final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return new LimestoneEnumerator<>(arrowTable, parquetTable, resultRowType);
            }
        };
    }

    public Enumerable<Object> runQuery(final List<String> fields, final String predicate) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
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
                return new LimestoneEnumerator(arrowTable, parquetTable, resultRowType);
            }
        };
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new LimestoneTableScan(context.getCluster(), relOptTable, this.parquetTable, this.arrowTable, relOptTable.getRowType());
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    private List<Field> getFieldList(JSONObject tableSchema) {
        List<Field> fieldList = new ArrayList<>();

        for(Object oName : (JSONArray) tableSchema.get("columns")) {
            String name = (String) ((JSONObject) oName ).get("name");
            String type = (String) ((JSONObject) oName ).get("type");

            ArrowFieldType arrowFieldType = ArrowFieldType.ofSimple(type);

            fieldList.add(new Field(name.toUpperCase(), FieldType.nullable(arrowFieldType.getArrowType()), new ArrayList<>()));
        }

        return fieldList;
    }

    private List<VectorSchemaRoot> getArrowSchema(JSONObject tableSchema) throws IOException {
        List<VectorSchemaRoot> vectorSchemaRoots = new ArrayList<>();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/test.arrow"));

        List<Field> fieldList = getFieldList(tableSchema);

        Schema schema = new Schema(fieldList);

        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));

        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream);

        arrowStreamWriter.start();

        int size = 1000;

        vectorSchemaRoot.setRowCount(size);

        for(FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            if(fieldVector.getClass() == IntVector.class) {
                IntVector vector = (IntVector) fieldVector;

                vector.setInitialCapacity(size);
                vector.allocateNew();

                for(int i = 0; i < size; i++){
                    vector.setSafe(i, i * i);
                }

                fieldVector.setValueCount(size);
            } else if(fieldVector.getClass() == VarCharVector.class) {
                VarCharVector vector = (VarCharVector) fieldVector;

                vector.setInitialCapacity(size);
                vector.allocateNew();

                for(int i = 0; i < size; i++){
                    vector.setSafe(i, new Text("ARROW"));
                }

                fieldVector.setValueCount(size);
            } else if(fieldVector.getClass() == Float8Vector.class) {
                Float8Vector vector = (Float8Vector) fieldVector;

                vector.setInitialCapacity(size);
                vector.allocateNew();

                for(int i = 0; i < size; i++){
                    vector.setSafe(i, 0.0);
                }

                fieldVector.setValueCount(size);
            } else {
                System.out.println("BLA");
            }
        }

        arrowStreamWriter.writeBatch();

        vectorSchemaRoots.add(vectorSchemaRoot);

        return vectorSchemaRoots;
    }
}
