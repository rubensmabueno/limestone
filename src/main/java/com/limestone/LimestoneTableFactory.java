//package com.limestone;
//
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeImpl;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.schema.TableFactory;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Map;
//
//public class LimestoneTableFactory implements TableFactory<LimestoneTable> {
//    public LimestoneTableFactory() {}
//
//    public LimestoneTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
//        LimestoneScannableTable table = null;
//
//        String directoryName = (String) operand.get("directory");
//        File[] files = new File(directoryName).listFiles();
//
//        for (File file : files) {
//            try {
//                Object jsonSchema = new JSONParser().parse(file.toString());
//                JSONObject tableSchema = (JSONObject) jsonSchema;
//
//                table = new LimestoneScannableTable(tableSchema, RelDataTypeImpl.proto(rowType));
//            } catch (ParseException | IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        return table;
//    }
//}
