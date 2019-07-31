package com.limestone;

import com.limestone.adapter.arrow.ArrowFieldType;
import com.limestone.adapter.parquet.ParquetFieldType;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.HashMap;
import java.util.Map;

public enum LimestoneFieldType {
    STRING(String.class, ArrowFieldType.STRING, ParquetFieldType.STRING, "string"),
    BOOLEAN(Primitive.BOOLEAN, ArrowFieldType.BOOLEAN, ParquetFieldType.BOOLEAN),
    INT(Primitive.INT, ArrowFieldType.INT, ParquetFieldType.INT),
    LONG(Primitive.LONG, ArrowFieldType.LONG, ParquetFieldType.LONG),
    FLOAT(Primitive.FLOAT, ArrowFieldType.FLOAT, ParquetFieldType.FLOAT),
    DOUBLE(Primitive.DOUBLE, ArrowFieldType.DOUBLE, ParquetFieldType.DOUBLE);
//    DATE(java.sql.Date.class, ArrowFieldType.DATE, ParquetFieldType.DATE, "date"),
//    TIME(java.sql.Time.class, ArrowFieldType.TIME, ParquetFieldType.TIME, "time"),
//    TIMESTAMP(java.sql.Timestamp.class, ArrowFieldType.TIMESTAMP, ParquetFieldType.TIMESTAMP, "timestamp");

    private final Class primitiveClass;
    private final ArrowFieldType arrowFieldType;
    private final ParquetFieldType parquetFieldType;
    private final String simpleName;

    private static final Map<String, LimestoneFieldType> MAP = new HashMap<>();

    static {
        for (LimestoneFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    LimestoneFieldType(Primitive primitive, ArrowFieldType arrowFieldType, ParquetFieldType parquetFieldType) {
        this(primitive.boxClass, arrowFieldType, parquetFieldType, primitive.primitiveName);
    }

    LimestoneFieldType(Class primitive, ArrowFieldType arrowFieldType, ParquetFieldType parquetFieldType, String simpleName) {
        this.primitiveClass = primitive;
        this.arrowFieldType = arrowFieldType;
        this.parquetFieldType = parquetFieldType;
        this.simpleName = simpleName;
    }

    public RelDataType toType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(primitiveClass);
    }

    public static LimestoneFieldType of(String simpleName) {
        return MAP.get(simpleName);
    }
}
