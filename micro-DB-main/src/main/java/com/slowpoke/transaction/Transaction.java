package com.slowpoke.transaction;

import com.slowpoke.annotation.VisibleForTest;
import com.slowpoke.connection.Connection;
import com.slowpoke.model.DataBase;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;

import java.util.List;

/**
 * Transcation
 *
 * @author zhangjw
 * @version 1.0
 */
public class Transaction {
    /**
     * transaction id
     */
    private TransactionID transactionId;

    /**
     * transaction lock
     */
    private Lock.LockType lockType;

    /**
     * constructor for transaction
     */
    public Transaction(Lock.LockType lockType) {
        this.transactionId = new TransactionID();
        this.lockType = lockType;
    }

    public TransactionID getTransactionId() {
        return transactionId;
    }

    public Lock.LockType getLockType() {
        return lockType;
    }

    @VisibleForTest
    public void setLockType(Lock.LockType lockType) {
        this.lockType = lockType;
    }

    public void start() {

    }


    public void commit() {

        List<PageID> pageIDs = DataBase.getLockManager().getPageIDs(transactionId);
        for (PageID pageID : pageIDs) {
            Page page = DataBase.getBufferPool().getPage(pageID);
            page.saveBeforePage();
        }

        DataBase.getLockManager().releaseLock(transactionId);

        Connection.clearTransaction();
    }

    public void abort() {


        DataBase.getLockManager().releaseLock(transactionId);

        Connection.clearTransaction();
    }

    @Override
    public String toString() {
        return "[" +
                "transactionId=" + transactionId +
                ", lockType=" + lockType +
                ']';
    }
}

