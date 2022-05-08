package com.slowpoke.transaction;

import com.slowpoke.model.page.PageID;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author zhangjw
 * @version 1.0
 */
public class Lock {

    public enum LockType {

        SLock,

        XLock
    }

    private PageID pageID;


    private LockType lockType;


    private Set<TransactionID> lockHolders;

    public Lock(PageID pageID, LockType lockType) {
        this.pageID = pageID;
        this.lockType = lockType;
        lockHolders = new HashSet<>();
    }


    public void setLockType(LockType lockType) {
        this.lockType = lockType;
    }

    public LockType getLockType() {
        return lockType;
    }


    public void addHolder(TransactionID transactionID) {
        lockHolders.add(transactionID);
    }

    public Set<TransactionID> getLockHolders() {
        return lockHolders;
    }
}
