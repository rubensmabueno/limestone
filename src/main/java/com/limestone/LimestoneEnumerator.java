package com.limestone;

import com.limestone.adapter.arrow.ArrowEnumerator;
import com.limestone.adapter.arrow.ArrowTable;
import com.limestone.adapter.parquet.ParquetEnumerator;
import com.limestone.adapter.parquet.ParquetTable;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LimestoneEnumerator<E> implements Enumerator<E> {
    private final ArrowEnumerator arrowEnumerator;
    private final ParquetEnumerator parquetEnumerator;
    private boolean arrowRemains;

    private final ArrowTable arrowTable;
    private final ParquetTable parquetTable;

    public LimestoneEnumerator(ArrowTable arrowTable, ParquetTable parquetTable, RelProtoDataType protoRowType) {
        this.arrowTable = arrowTable;
        this.parquetTable = parquetTable;
        this.arrowRemains = true;

        this.arrowEnumerator = new ArrowEnumerator(this.arrowTable.getVectorSchemaRoots(), new AtomicBoolean(false), protoRowType);
        this.parquetEnumerator = new ParquetEnumerator(this.parquetTable.getFile(), new AtomicBoolean(false), protoRowType);
    }

    public static RelDataType deduceRowType(RelDataTypeFactory typeFactory, JSONObject tableSchema) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        for(Object oName : (JSONArray) tableSchema.get("columns")) {
            String name = (String) ((JSONObject) oName ).get("name");
            String type = (String) ((JSONObject) oName ).get("type");

            RelDataType relDataType = LimestoneFieldType.of(type).toType(typeFactory);

            names.add(name.toUpperCase());
            types.add(relDataType);
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    @Override public void reset() {}

    @Override public void close() {}

    @Override public boolean moveNext() {
        this.arrowRemains = this.arrowEnumerator.moveNext();

        if(!this.arrowRemains) {
            return this.parquetEnumerator.moveNext();
        } else {
            return this.arrowRemains;
        }
    }

    @Override public E current() {
        if(this.arrowRemains) {
            return (E) this.arrowEnumerator.current();
        } else {
            return (E) this.parquetEnumerator.current();
        }
    }
}


