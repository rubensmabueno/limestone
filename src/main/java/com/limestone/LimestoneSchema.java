package com.limestone;

import com.google.common.collect.ImmutableMap;
import com.limestone.adapter.arrow.ArrowFieldType;
import com.limestone.adapter.arrow.ArrowTranslatableTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class LimestoneSchema extends AbstractSchema {
    private final File schemaDirectory;
    private Map<String, Table> tableMap;

    public LimestoneSchema(File schemaDirectory) {
        super();

        this.schemaDirectory = schemaDirectory;
    }

    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        File[] files = schemaDirectory.listFiles();

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        for (File file : files) {
            try {
                Object schema = new JSONParser().parse(new FileReader(file));
                JSONObject tableSchema = (JSONObject) schema;

                builder.put(file.getName().replace(".json", "").toUpperCase(), new ArrowTranslatableTable(arrowSchema(tableSchema), null));
            } catch (ParseException | IOException e) {
                e.printStackTrace();
            }
        }

        return builder.build();
    }

    public List<VectorSchemaRoot> arrowSchema(JSONObject tableSchema) throws IOException {
        List<VectorSchemaRoot> vectorSchemaRoots = new ArrayList<>();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/test.arrow"));

        List<Field> fieldList = new ArrayList<>();

        for(Object oName : tableSchema.keySet()) {
            String name = (String) oName;
            String type = (String) tableSchema.get(name);

            ArrowFieldType arrowFieldType = ArrowFieldType.ofSimple(type);

            fieldList.add(new Field(name, FieldType.nullable(arrowFieldType.getArrowType()), new ArrayList<>()));
        }

        Schema schema = new Schema(fieldList);

        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));

        ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream);

        arrowStreamWriter.start();

        int size = 30000000;

        vectorSchemaRoot.setRowCount(size);

        for(FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
            IntVector vector = (IntVector) fieldVector;

            vector.setInitialCapacity(size);
            vector.allocateNew();

            for(int i = 0; i < size; i++){
                vector.setSafe(i, i * i);
            }

            fieldVector.setValueCount(size);
        }

        arrowStreamWriter.writeBatch();

        vectorSchemaRoots.add(vectorSchemaRoot);

        return vectorSchemaRoots;
    }
}
