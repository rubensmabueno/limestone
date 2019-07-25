package com.limestone;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

public class LimestoneSchemaFactory implements SchemaFactory {
    public static final LimestoneSchemaFactory INSTANCE = new LimestoneSchemaFactory();

    private LimestoneSchemaFactory() {
    }

    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final String directory = (String) operand.get("directory");
        final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY);

        File schemaDirectory = new File(directory);

        if (base != null && !schemaDirectory.isAbsolute()) {
            schemaDirectory = new File(base, directory);
        }

        return new LimestoneSchema(schemaDirectory);
    }
}
