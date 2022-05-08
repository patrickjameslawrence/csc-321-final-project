package com.slowpoke.model.page;

import com.slowpoke.transaction.TransactionID;

import java.io.IOException;

/**
 * Page
 *
 * @author zhangjw
 * @version 1.0
 */
public interface Page {

    PageID getPageID();

    /**
     * serialize page data
     */
    byte[] serialize() throws IOException;

    /**
     * deserialize page data
     */
    void deserialize(byte[] pageData) throws IOException;



    int getMaxSlotNum();

    boolean isSlotUsed(int index);

    boolean hasEmptySlot();

    /**
     * mark if dirty
     */
    void markDirty(TransactionID transactionID);

    boolean isDirty();

    TransactionID getDirtyTxId();

    /**
     * keep page original data
     */
    void saveBeforePage();
    /**
     *
     */
    Page getBeforePage();
}
