package com.slowpoke.model.field;

import com.slowpoke.exception.ParseException;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * field type
 *
 * @author zhangjw, Dr. Chen
 * @version 1.0
 */
public enum FieldType implements IFieldType {
    INT() {
        @Override
        public int getSizeInByte() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                return new IntField(dis.readInt());
            } catch (IOException e) {
                throw new ParseException("parse field failed", e);
            }
        }
    },

    LONG() {
        @Override
        public int getSizeInByte() {
            return 8;
        }

        @Override
        public Field parse(DataInputStream dis) {
            try {
                return new LongField(dis.readLong());
            } catch (IOException e) {
                throw new ParseException("parse field failed", e);
            }
        }
    }
}
