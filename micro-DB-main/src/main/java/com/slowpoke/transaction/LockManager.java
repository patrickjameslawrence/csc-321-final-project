package com.slowpoke.transaction;

import com.slowpoke.bufferpool.BufferPool;
import com.slowpoke.exception.TransactionException;
import com.slowpoke.model.DataBase;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * lock manager
 *
 * @author zhangjw
 * @version 1.0
 */
public class LockManager {


    /**
     * default time out for each lock
     */
    public static final int defaultTimeOutInMillis = 10000;
    /**
     * all locks
     */
    private final ConcurrentHashMap<PageID, Lock> lockTable;

    /**
     * all transactions
     */
    private final ConcurrentHashMap<TransactionID, List<PageID>> transactionTable;


    public LockManager() {
        lockTable = new ConcurrentHashMap<>();
        transactionTable = new ConcurrentHashMap<>();
    }

    public int getDefaultTimeOutInMillis() {
        return defaultTimeOutInMillis;
    }

    /**
     * acquire lock
     */
    public synchronized void acquireLock(Transaction transaction, PageID pageID)
            throws TransactionException {
        //System.out.println(String.format("Acquiring Lock,transaction=%s,pageID=%s", transaction, pageID));
        TransactionID currentTransId = transaction.getTransactionId();

        if (Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {

            while (true) {
                if (!lockTable.containsKey(pageID)) {
                    grantLock(transaction, pageID, currentTransId);
                    return;
                } else {
                    if (lockTable.get(pageID).getLockHolders().contains(currentTransId)) {
                        Lock lock = lockTable.get(pageID);
                        if (Objects.equals(lock.getLockType(), Lock.LockType.SLock)) {
                            lock.setLockType(Lock.LockType.XLock);
                            lockTable.put(pageID, lock);
                        }
                        return;
                    } else {
                        block(System.currentTimeMillis());
                    }
                }
            }
        } else {
            while (true) {
                if (!lockTable.containsKey(pageID)) {
                    grantLock(transaction, pageID, currentTransId);
                    return;
                } else {
                    Lock existingLock = lockTable.get(pageID);
                    if (Objects.equals(existingLock.getLockType(), Lock.LockType.XLock)) {
                        if (existingLock.getLockHolders().size() == 1
                                && existingLock.getLockHolders().contains(currentTransId)) {
                            return;
                        } else {
                            block(System.currentTimeMillis());
                        }
                    } else {
                        existingLock.addHolder(currentTransId);
                        return;
                    }
                }
            }
        }
    }

    private void grantLock(Transaction transaction, PageID pageID, TransactionID currentTransId) {
        Lock existingLock = new Lock(pageID, transaction.getLockType());
        existingLock.addHolder(currentTransId);
        lockTable.put(pageID, existingLock);

        List<PageID> pageIDS = transactionTable.getOrDefault(currentTransId, new ArrayList<>());
        pageIDS.add(pageID);
        transactionTable.put(currentTransId, pageIDS);
        return;
    }

    private void block(long startTime) {
        try {
            wait(LockManager.defaultTimeOutInMillis);

            if (System.currentTimeMillis() - startTime > LockManager.defaultTimeOutInMillis) {
                throw new TransactionException("Time out");
            }
        } catch (InterruptedException e) {
            throw new TransactionException("object.wait() exception");
        }
    }

    /**
     * release lock
     */
    public synchronized void releaseLock(TransactionID transactionID)
            throws TransactionException {
        //System.out.println(String.format("transactionID:%s released lock", transactionID));

        List<PageID> pageIDS = transactionTable.get(transactionID);
        if (pageIDS != null && !pageIDS.isEmpty()) {
            for (PageID pageID : pageIDS) {
                lockTable.remove(pageID);
            }
        }
        transactionTable.remove(transactionID);

        notifyAll();
    }

    public List<PageID> getPageIDs(TransactionID transactionId) {
        return transactionTable.get(transactionId);
    }

    public List<Page> getPages(TransactionID transactionId) {
        List<PageID> pageIDS = transactionTable.get(transactionId);
        BufferPool bufferPool = DataBase.getBufferPool();

        if (pageIDS != null ) {
            return pageIDS.stream()
                    .map(bufferPool::getPage)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }
}
