package com.limestone;

import com.google.common.collect.ImmutableMap;
import com.limestone.adapter.arrow.ArrowFieldType;
import com.limestone.adapter.arrow.ArrowTranslatableTable;
import com.limestone.adapter.parquet.ParquetTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
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
                builder.put(file.getName().toUpperCase(), new LimestoneTable(file));
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }

//            if(file.getName().contains(".json")) {
//                try {
//                    Object schema = new JSONParser().parse(new FileReader(file));
//                    JSONObject tableSchema = (JSONObject) schema;
//
//                    builder.put(file.getName().replace(".json", "").toUpperCase(), new ArrowTranslatableTable(arrowSchema(tableSchema), null));
//                } catch (ParseException | IOException e) {
//                    e.printStackTrace();
//                }
//            } else {
//                try {
//                    builder.put(file.getName().replace(".parquet", "").toUpperCase(), new ParquetTable(file));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        }

        return builder.build();
    }
}
