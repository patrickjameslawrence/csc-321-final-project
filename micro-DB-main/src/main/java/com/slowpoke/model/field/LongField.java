package com.slowpoke.model.field;


import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Long field
 *
 * @author zhangjw
 * @version 1.0
 */
public class LongField implements Field {
    private static final long serialVersionUID = 6463445416908424053L;

    private final long value;

    public LongField(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public FieldType getType() {
        return FieldType.LONG;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeLong(value);
    }


}
