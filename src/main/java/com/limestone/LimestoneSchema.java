package com.limestone;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
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
        }

        return builder.build();
    }
}
