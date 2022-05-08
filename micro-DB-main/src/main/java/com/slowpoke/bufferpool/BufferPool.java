package com.slowpoke.bufferpool;

import com.slowpoke.annotation.VisibleForTest;
import com.slowpoke.config.DBConfig;
import com.slowpoke.connection.Connection;
import com.slowpoke.exception.DbException;
import com.slowpoke.exception.TransactionException;
import com.slowpoke.model.DataBase;
import com.slowpoke.model.table.DbTable;
import com.slowpoke.model.row.Row;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;
import com.slowpoke.transaction.Lock;
import com.slowpoke.transaction.Transaction;
import com.slowpoke.transaction.TransactionID;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * buffer pool
 *
 * @author Dr. Chen
 * @version 1.1
 */
public class BufferPool {

    private final ConcurrentHashMap<PageID, Page> pool;

    /**
     * capacity: max # of pages per pool
     */
    private final int capacity;

    public BufferPool(DBConfig dbConfig) {
        this.capacity = dbConfig.getBufferPoolCapacity();
        pool = new ConcurrentHashMap<>();
    }


    /**
     * read page
     * First try the buffer if not in buffer pool then read from disk
     * if buffer pool is full then evict pages inside the pool
     * @param pageID pageID
     */
    public Page getPage(PageID pageID) {
        Transaction currentTransaction = Connection.currentTransaction();
        try {
            DataBase.getLockManager().acquireLock(currentTransaction, pageID);
        } catch (TransactionException e) {
            System.out.println(String.format("acquire lock failed,transaction=%s,pageID=%s", currentTransaction, pageID));
            throw e;
        }

       // System.out.println(String.format("acquire lock success: page ID %s", pageID));

        Page page = pool.get(pageID);
        if (page == null) {
            if (isFull()) {
                // evict page
                this.evictPages();
            }
            page = DataBase.getInstance().getDbTableById(pageID.getTableId()).getTableFile().readPageFromDisk(pageID);
            pool.put(pageID, page);
        }
        return page;
    }

    public void insertRow(Row row, String tableName) throws IOException {
        Transaction transaction = Connection.currentTransaction();
        if (!Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            throw new TransactionException("x lock must be granted in updating");
        }

        DbTable dbTable = DataBase.getInstance().getDbTableByName(tableName);
        dbTable.insertRow(row);

        // get dirty pages from the insert command
        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        if (dirtyPages.isEmpty()) {
            throw new DbException("insert data create dirty page");
        }
        System.out.println("dirtyPages.size():" + dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
        Connection.clearDirtyPages();
    }

    public void deleteRow(Row row) throws IOException {
        Transaction transaction = Connection.currentTransaction();
        if (!Objects.equals(transaction.getLockType(), Lock.LockType.XLock)) {
            throw new TransactionException("x lock must be granted in updating");
        }
        DbTable dbTable = DataBase.getInstance().getDbTableById(row.getRowID().getPageID().getTableId());
        dbTable.deleteRow(row);

        HashMap<PageID, Page> dirtyPages = Connection.getDirtyPages();
        System.out.println("del.dirtyPages.size():" + dirtyPages.size());
        for (Map.Entry<PageID, Page> entry : dirtyPages.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
        Connection.clearDirtyPages();
    }

    /**
     * check if buffer pool is full
     */
    private boolean isFull() {
        return pool.size() >= capacity;
    }

    /**
     * evict page, remove pages from buffer pool
     * <p>
     * Current code is going to remove all pages from the buffer pool --> which is not ideal.
     * TODO: Please Implement the Clock algorithm
     *
     */
    private void evictPages() {
        for (Map.Entry<PageID, Page> entry : pool.entrySet()) {
            Page page = entry.getValue();

            //flushPage(page);
            // remove page from buffer pool
            pool.remove(page.getPageID());
            System.out.printf("evict pages: %d\n", page.getPageID().getPageNo());
        }
        System.out.printf("Done\n");

    }

    /**
     * flush page back to hard disk
     */
    private void flushPage(Page page) {
        int tableId = page.getPageID().getTableId();
        DataBase.getInstance().getDbTableById(tableId).getTableFile().writePageToDisk(page);
    }

    public void flushPages(List<PageID> pageIDs, TransactionID transactionId) {
        if (Objects.isNull(pageIDs) || pageIDs.isEmpty()) {
            System.out.println("flushPages : pageIDs is null or empty ");
            return;
        }
        for (PageID pageID : pageIDs) {
            Page page = pool.get(pageID);
            if (page.isDirty()) {
                page.markDirty(null);
                pool.put(pageID, page);
                flushPage(page);
            }
        }
    }

    /**
     * discard pages if not dirty
     */
    public void discardPages(List<PageID> pageIDs) {
        if (Objects.isNull(pageIDs) || pageIDs.isEmpty()) {
            System.out.println("discardPages : pageIDs is null or empty ");
            return;
        }
        for (PageID pageID : pageIDs) {
            pool.remove(pageID);
        }
    }

    /**
     * flush all page
     * for test only
     */
    @VisibleForTest
    public void flushAllPage() {
        for (Map.Entry<PageID, Page> entry : pool.entrySet()) {
            flushPage(entry.getValue());
        }
    }
}
