package com.slowpoke.model.page.heap;

import com.slowpoke.model.page.PageID;

import java.io.Serializable;
import java.util.Objects;

/**
 * pageID
 *
 * @author zhangjw, Dr. CHen
 * @version 1.0
 */
public class HeapPageID implements PageID, Serializable {
    private static final long serialVersionUID = -1414021678836415955L;
    /**
     * table id
     */
    private int tableId;

    /**
     * page no
     */
    private int pageNo;

    public HeapPageID(int tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeapPageID that = (HeapPageID) o;
        return tableId == that.tableId &&
                pageNo == that.pageNo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNo);
    }

    @Override
    public String toString() {
        return "HeapPageID{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                '}';
    }
}
