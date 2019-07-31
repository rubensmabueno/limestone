package com.limestone.adapter.parquet;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

public enum ParquetFieldType {
    STRING(String.class, PrimitiveType.PrimitiveTypeName.BINARY, "string"),
    BOOLEAN(Primitive.BOOLEAN, PrimitiveType.PrimitiveTypeName.BOOLEAN),
    INT(Primitive.INT, PrimitiveType.PrimitiveTypeName.INT32),
    LONG(Primitive.LONG, PrimitiveType.PrimitiveTypeName.INT96),
    FLOAT(Primitive.FLOAT, PrimitiveType.PrimitiveTypeName.FLOAT),
    DOUBLE(Primitive.DOUBLE, PrimitiveType.PrimitiveTypeName.DOUBLE);

    private final Class primitiveClass;
    private final PrimitiveType.PrimitiveTypeName parquetClass;
    private final String simpleName;

    private static final Map<PrimitiveType.PrimitiveTypeName, ParquetFieldType> MAP = new HashMap<>();

    static {
        for (ParquetFieldType value : values()) {
            MAP.put(value.parquetClass, value);
        }
    }

    ParquetFieldType(Primitive primitive, PrimitiveType.PrimitiveTypeName parquetClass) {
        this(primitive.boxClass, parquetClass, primitive.primitiveName);
    }

    ParquetFieldType(Class primitiveClass, PrimitiveType.PrimitiveTypeName parquetClass, String simpleName) {
        this.primitiveClass = primitiveClass;
        this.parquetClass = parquetClass;
        this.simpleName = simpleName;
    }

    public RelDataType toType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(primitiveClass);
    }

    public static ParquetFieldType of(PrimitiveType.PrimitiveTypeName parquetType) {
        return MAP.get(parquetType);
    }
}
