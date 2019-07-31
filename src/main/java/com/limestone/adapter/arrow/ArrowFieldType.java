package com.limestone.adapter.arrow;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;

import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

public enum ArrowFieldType {
    STRING(String.class, ArrowType.Utf8.class, "string"),
    BOOLEAN(Primitive.BOOLEAN, ArrowType.Bool.class),
    INT(Primitive.INT, ArrowType.Int.class),
    LONG(Primitive.LONG, ArrowType.Int.class, "long"),
    FLOAT(Primitive.FLOAT, ArrowType.FloatingPoint.class),
    DOUBLE(Primitive.DOUBLE, ArrowType.FloatingPoint.class, "double"),
    DATE(java.sql.Date.class, ArrowType.Date.class, "date"),
    TIME(java.sql.Time.class, ArrowType.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, ArrowType.Timestamp.class, "timestamp");

    private final Class primitiveClass;
    private final Class arrowClass;
    private final String simpleName;

    private static final Map<Class, ArrowFieldType> MAP = new HashMap<>();
    private static final Map<String, ArrowFieldType> SIMPLEMAP = new HashMap<>();

    static {
        for (ArrowFieldType value : values()) {
            MAP.put(value.arrowClass, value);
            SIMPLEMAP.put(value.simpleName, value);
        }
    }

    ArrowFieldType(Primitive primitive, Class arrowClass) {
        this(primitive.boxClass, arrowClass, primitive.primitiveName);
    }

    ArrowFieldType(Primitive primitive, Class arrowClass, String simpleName) {
        this.primitiveClass = primitive.boxClass;
        this.arrowClass = arrowClass;
        this.simpleName = simpleName;
    }

    ArrowFieldType(Class primitiveClass, Class arrowClass, String simpleName) {
        this.primitiveClass = primitiveClass;
        this.arrowClass = arrowClass;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        return typeFactory.createJavaType(primitiveClass);
    }

    public static ArrowFieldType of(ArrowType arrowType) {
        return MAP.get(arrowType.getClass());
    }

    public static ArrowFieldType ofSimple(String simpleName) {
        return SIMPLEMAP.get(simpleName);
    }

    public ArrowType getArrowType() {
        Object arrowType = null;

        try {
            if (this.arrowClass == ArrowType.Int.class) {
                arrowType = new ArrowType.Int(32, true);
            } else if (this.primitiveClass == Primitive.DOUBLE.boxClass) {
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            } else {
                arrowType = this.arrowClass.newInstance();
            }
        }
        catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        return (ArrowType) arrowType;
    }
}
