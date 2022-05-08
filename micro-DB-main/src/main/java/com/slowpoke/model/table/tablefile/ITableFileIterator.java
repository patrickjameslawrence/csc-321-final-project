package com.slowpoke.model.table.tablefile;

import com.slowpoke.exception.DbException;
import com.slowpoke.model.row.Row;

import java.util.NoSuchElementException;

/**
 * file iterator interface
 *
 * @author zhangjw
 * @version 1.0
 */
public interface ITableFileIterator {

    void open() throws DbException;

    boolean hasNext() throws DbException;

    Row next() throws DbException, NoSuchElementException;

    void close();
}
