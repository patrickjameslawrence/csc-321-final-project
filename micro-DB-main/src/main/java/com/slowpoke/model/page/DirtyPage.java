package com.slowpoke.model.page;

import com.slowpoke.transaction.TransactionID;

/**
 * class for maintaining dirty page
 *
 * @author zhangjw
 * @version 1.0
 */
public abstract class DirtyPage implements Page {
    /**
     * if page been modified, then a transaction ID will be set and we use transaction ID to find dirty page.
     */
    protected TransactionID dirtyTxId = null;

    @Override
    public void markDirty(TransactionID transactionID) {
        dirtyTxId = transactionID;
    }

    @Override
    public boolean isDirty() {
        return dirtyTxId != null;
    }

    @Override
    public TransactionID getDirtyTxId() {
        return dirtyTxId;
    }
}
