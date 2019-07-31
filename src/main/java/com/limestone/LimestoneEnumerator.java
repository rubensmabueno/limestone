package com.limestone;

import com.limestone.adapter.arrow.ArrowEnumerator;
import com.limestone.adapter.parquet.ParquetEnumerator;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.Pair;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class LimestoneEnumerator<E> implements Enumerator<E> {
    private final ArrowEnumerator arrowEnumerator;
    private final ParquetEnumerator parquetEnumerator;

    public LimestoneEnumerator(RelProtoDataType protoRowType) {
        this.arrowEnumerator = new ArrowEnumerator();
        this.parquetEnumerator = new ParquetEnumerator();
    }

    public static RelDataType deduceRowType(RelDataTypeFactory typeFactory, JSONObject tableSchema) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        for(Object keyObject : tableSchema.keySet()) {
            String name = (String) keyObject;
            String type = (String) tableSchema.get(keyObject);

            RelDataType relDataType = LimestoneFieldType.of(type).toType(typeFactory);

            names.add(name.toUpperCase());
            types.add(relDataType);
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public void reset() {}

    @Override public void close() {}

    @Override public boolean moveNext() {}

    @Override public E current() {}
}


