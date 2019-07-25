//package com.limestone;
//
//import com.limestone.adapter.arrow.ArrowFieldType;
//import com.limestone.adapter.arrow.ArrowTable;
//import org.apache.arrow.memory.RootAllocator;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.UInt1Vector;
//import org.apache.arrow.vector.VectorSchemaRoot;
//import org.apache.arrow.vector.ipc.ArrowStreamWriter;
//import org.apache.arrow.vector.types.pojo.Field;
//import org.apache.arrow.vector.types.pojo.FieldType;
//import org.apache.arrow.vector.types.pojo.Schema;
//import org.apache.calcite.adapter.java.JavaTypeFactory;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelProtoDataType;
//import org.apache.calcite.schema.impl.AbstractTable;
//import org.json.simple.JSONObject;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public abstract class LimestoneTable extends AbstractTable {
//  protected final JSONObject tableSchema;
//  protected final RelProtoDataType protoRowType;
//  protected ArrowTable arrowTable;
//
//  LimestoneTable(JSONObject tableSchema, RelProtoDataType protoRowType) {
//    this.tableSchema = tableSchema;
//    this.protoRowType = protoRowType;
//  }
//
//  public List<VectorSchemaRoot> arrowSchema() throws IOException {
//    List<VectorSchemaRoot> vectorSchemaRoots = new ArrayList<>();
//    FileOutputStream fileOutputStream = new FileOutputStream(new File("/tmp/test.arrow"));
//
//    List<Field> fieldList = new ArrayList<>();
//
//    for(Object oName : tableSchema.keySet()) {
//      String name = (String) oName;
//      String type = (String) tableSchema.get(name);
//
//      ArrowFieldType arrowFieldType = ArrowFieldType.ofSimple(type);
//
//      fieldList.add(new Field(name, FieldType.nullable(arrowFieldType.build()), new ArrayList<>()));
//    }
//
//    Schema schema = new Schema(fieldList);
//
//    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));
//
//    ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, fileOutputStream);
//
//    arrowStreamWriter.start();
//
//    int size = 3;
//
//    vectorSchemaRoot.setRowCount(size);
//
//    for(FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
//      UInt1Vector vector = (UInt1Vector) fieldVector;
//
//      vector.setInitialCapacity(size);
//      vector.allocateNew();
//
//      for(int i = 0; i < size; i++){
//        vector.setSafe(i, i * i);
//      }
//
//      fieldVector.setValueCount(size);
//    }
//
//    arrowStreamWriter.writeBatch();
//
//    vectorSchemaRoots.add(vectorSchemaRoot);
//
//    return vectorSchemaRoots;
//  }
//
//  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//    return getRowType((JavaTypeFactory) typeFactory);
//  }
//
//  public RelDataType getRowType(JavaTypeFactory typeFactory) {
//    return this.arrowTable.getRowType(typeFactory);
//  }
//}
