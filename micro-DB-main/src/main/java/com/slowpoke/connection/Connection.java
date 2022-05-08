package com.slowpoke.connection;

import com.slowpoke.exception.DbException;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;
import com.slowpoke.transaction.Lock;
import com.slowpoke.transaction.Transaction;

import java.util.HashMap;
import java.util.Objects;

/**
 * tracking dirty page
 *
 * @author zhangjw, Dr. Chen
 * @version 1.0
 */
public class Connection {
    private static final String DIRTY_PAGE_KEY = "dp";
    private static final String TRANSACTION_KEY = "tx";
    private static final ThreadLocal<HashMap<String, Object>> connection = new ThreadLocal<>();

    /**
     */
    public static void passingTransaction(Transaction transaction) {
        HashMap<String, Object> map = getOrInitThreadMap();
        map.put(TRANSACTION_KEY, transaction);
    }

    /**
     * get current transaction
     */
    public static Transaction currentTransaction() {
        HashMap<String, Object> map = getOrInitThreadMap();
        Transaction transaction = (Transaction) map.get(TRANSACTION_KEY);

        if (transaction == null) {
            throw new DbException("error:not open");
        }

        return transaction;
    }

    /**
     * clear transaction from threadlocal
     */
    public static void clearTransaction() {
        HashMap<String, Object> map = getOrInitThreadMap();
        map.remove(TRANSACTION_KEY);
    }

    /**
     * dirty page from update
     */
    public static void cacheDirtyPage(Page page) {
        Transaction transaction = Connection.currentTransaction();
        if (Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            HashMap<String, Object> map = getOrInitThreadMap();
            HashMap<PageID, Page> pages =
                    (HashMap<PageID, Page>) map.compute(DIRTY_PAGE_KEY, (k, v) -> v == null ? v = new HashMap<>() : v);
            page.markDirty(transaction.getTransactionId());
            pages.put(page.getPageID(), page);
        }
    }

    /**
     * get dirty page from thread memory
     *
     */
    public static HashMap<PageID, Page> getDirtyPages() {
        HashMap<String, Object> map = getOrInitThreadMap();
        return (HashMap<PageID, Page>) map.get(DIRTY_PAGE_KEY);


    }
    /**
     * clean dirty page from thread memory
     */
    public static void clearDirtyPages() {
        HashMap<String, Object> map = connection.get();
        map.put(DIRTY_PAGE_KEY, new HashMap<>());
    }

    private static HashMap<String, Object> getOrInitThreadMap() {
        HashMap<String, Object> map = connection.get();
        if (map == null) {
            map = new HashMap<>();
            connection.set(map);
            return map;
        }
        return map;
    }
}
