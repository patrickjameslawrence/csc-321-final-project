package com.slowpoke.model.field;


import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * one field in tuple
 *
 * @author zhangjw
 * @version 1.0
 */
public interface Field extends Serializable {

    /**
     * return field type
     */
    FieldType getType();

    void serialize(DataOutputStream dos) throws IOException;

    String toString();


}
