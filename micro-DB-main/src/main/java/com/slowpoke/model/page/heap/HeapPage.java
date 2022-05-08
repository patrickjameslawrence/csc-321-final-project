package com.slowpoke.model.page.heap;

import com.slowpoke.exception.DbException;
import com.slowpoke.model.DataBase;
import com.slowpoke.model.row.Row;
import com.slowpoke.model.table.TableDesc;
import com.slowpoke.model.field.Field;
import com.slowpoke.model.page.DirtyPage;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;
import com.slowpoke.model.row.RowID;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Page, the basic unit for reading hard disk data.
 *
 * @author zhangjw
 * @version 1.0
 */
public class HeapPage extends DirtyPage implements Page, Serializable {

    private static final long serialVersionUID = 201055810875655321L;
    /**
     * page ID
     */
    private PageID pageID;
    /**
     * row data
     */
    private Row[] rows;
    /**
     * table structure
     */
    private TableDesc tableDesc;
    /**
     * max data row per page
     */
    private int maxSlotNum;
    /**
     * slot使用状态标识位图
     * 为利用 {@link DataOutputStream#writeBoolean(boolean)} api的便利性，
     * 物理文件使用一个byte存储一个状态位
     */
    private boolean[] slotUsageStatusBitMap;

    /**
     * Original Page data
     */
    private byte[] beforePageData;

    public HeapPage(PageID pageID, byte[] pageData) throws IOException {
        this.pageID = pageID;
        this.tableDesc = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableDesc();
        this.maxSlotNum = calculateMaxSlotNum(this.tableDesc);
        this.rows = new Row[this.maxSlotNum];
        this.slotUsageStatusBitMap = new boolean[this.maxSlotNum];
        deserialize(pageData);

        // save original page data
        saveBeforePage();
    }

    @Override
    public PageID getPageID() {
        return pageID;
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(DataBase.getDBConfig().getPageSizeInByte());
        DataOutputStream dos = new DataOutputStream(baos);

        for (boolean status : slotUsageStatusBitMap) {
            dos.writeBoolean(status);
        }

        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                for (Field field : rows[i].getFields()) {
                    field.serialize(dos);
                }
            } else {
                fillBytes(dos, tableDesc.getRowMaxSizeInBytes());
            }
        }

        int zeroSize =
                DataBase.getDBConfig().getPageSizeInByte()
                        - slotUsageStatusBitMap.length
                        - tableDesc.getRowMaxSizeInBytes() * rows.length;
        fillBytes(dos, zeroSize);
        dos.flush();
        return baos.toByteArray();
    }

    protected void fillBytes(DataOutputStream dos, int bytesNum) throws IOException {
        if (dos == null) {
            throw new DbException("fill bytes error: stream is closed ");
        }
        byte[] emptyBytes = new byte[bytesNum];
        dos.write(emptyBytes, 0, bytesNum);
    }

    /**
     * deserialize
     */
    public void deserialize(byte[] pageData) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(pageData));
        // slot status
        for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
            slotUsageStatusBitMap[i] = dis.readBoolean();
        }
        // row data
        rows = new Row[maxSlotNum];
        for (int i = 0; i < rows.length; i++) {
            if (isSlotUsed(i)) {
                Row row = new Row(this.tableDesc);
                Field[] fields = this.tableDesc.getFieldTypes()
                        .stream()
                        .map(x -> x.parse(dis))
                        .toArray(Field[]::new);
                row.setFields(fields);
                row.setRowID(new RowID(pageID, i));
                rows[i] = row;
            } else {
                rows[i] = null;
            }
        }

        dis.close();
    }

    @Override
    public boolean isSlotUsed(int i) {
        return slotUsageStatusBitMap[i];
    }

    private boolean isSlotEmpty(int i) {
        return !slotUsageStatusBitMap[i];
    }

    public static byte[] createEmptyPageData() {
        return new byte[DataBase.getDBConfig().getPageSizeInByte()];
    }

    @Override
    public boolean hasEmptySlot() {
        for (boolean b : this.slotUsageStatusBitMap) {
            if (!b) {
                return true;
            }
        }
        return false;
    }

    /**
     * calculate # of slots per page
     *
     */
    public int calculateMaxSlotNum(TableDesc tableDesc) {
        // slot status 1 byte
        int slotStatusSizeInByte = 1;
        return DataBase.getDBConfig().getPageSizeInByte() / (tableDesc.getRowMaxSizeInBytes() + slotStatusSizeInByte);
    }

    @Override
    public int getMaxSlotNum() {
        return maxSlotNum;
    }

    public void insertRow(Row row) {
        if (row == null) {
            throw new DbException("insert row error: row can not be null");
        }
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (!slotUsageStatusBitMap[i]) {
                slotUsageStatusBitMap[i] = true;
                row.setRowID(new RowID(pageID, i));
                rows[i] = row;
                return;
            }
        }
        throw new DbException("insert row error: no empty slot");
    }

    public void deleteRow(Row row) {
        if (row == null) {
            throw new DbException("delete row error: row can not be null");
        }

        int slotIndex = row.getRowID().getSlotIndex();
        slotUsageStatusBitMap[slotIndex] = false;
        rows[slotIndex] = null;
    }



    public Iterator<Row> getRowIterator() {
        return new RowIterator();
    }

    @Override
    public void saveBeforePage() {
        try {
            beforePageData = this.serialize().clone();
        } catch (Exception e) {
            throw new DbException("save before page error", e);
        }
    }

    @Override
    public HeapPage getBeforePage() {
        try {
            return new HeapPage(pageID, beforePageData);
        } catch (IOException e) {
            throw new DbException("get before page error", e);
        }
    }

    //====================================Iterator======================================

    private class RowIterator implements Iterator<Row> {
        private Iterator<Row> rowIterator;

        public RowIterator() {
            ArrayList<Row> rowList = new ArrayList<>();
            for (int i = 0; i < slotUsageStatusBitMap.length; i++) {
                if (isSlotUsed(i)) {
                    rowList.add(rows[i]);
                }
            }
            rowIterator = rowList.iterator();
        }

        @Override
        public boolean hasNext() {
            return rowIterator.hasNext();
        }

        @Override
        public Row next() {
            return rowIterator.next();
        }
    }
}
