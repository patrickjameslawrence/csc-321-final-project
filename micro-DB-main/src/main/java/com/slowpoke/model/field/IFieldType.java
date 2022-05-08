package com.slowpoke.model.field;

import java.io.DataInputStream;

/**
 * field type
 *
 * @author zhangjw
 * @version 1.0
 */
public interface IFieldType {

    /**
     * return size in byte
     */
    int getSizeInByte();

    /**
     * deserialized
     */
    Field parse(DataInputStream dis);
}
