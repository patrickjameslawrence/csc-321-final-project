package com.slowpoke.model;

import com.slowpoke.annotation.VisibleForTest;
import com.slowpoke.bufferpool.BufferPool;
import com.slowpoke.config.DBConfig;
import com.slowpoke.exception.DbException;

import com.slowpoke.model.table.DbTable;
import com.slowpoke.model.table.TableDesc;
import com.slowpoke.model.table.tablefile.TableFile;
import com.slowpoke.transaction.LockManager;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * database class
 *
 * @author zhangjw & Si Chen
 * @version 1.0
 */
public class DataBase {
    private static final AtomicReference<DataBase> singleton = new AtomicReference<>(new DataBase());

    /**
     * Config
     */
    private final DBConfig dbConfig;

    /**
     * dbTableFile id to DbTable
     */
    private final HashMap<Integer, DbTable> tableId2Table;
    private final HashMap<String, DbTable> tableName2Table;

    /**
     * buffer pool
     */
    private final BufferPool bufferPool;

    /**
     * lock manager
     */
    private final LockManager lockManager;



    private DataBase() {
        this.dbConfig = new DBConfig(4096,4);
        this.tableId2Table = new HashMap<>();
        this.tableName2Table = new HashMap<>();
        this.lockManager = new LockManager();
        this.bufferPool = new BufferPool(dbConfig);

    }

    public static DataBase getInstance() {
        return singleton.get();
    }

    public static DBConfig getDBConfig() {
        return singleton.get().dbConfig;
    }

    public static BufferPool getBufferPool() {
        return singleton.get().bufferPool;
    }

    public static LockManager getLockManager() {
        return singleton.get().lockManager;
    }


    /**
     * add table
     *
     * @param tableFile table file
     * @param tableName table name
     */
    @Deprecated
    public void addTable(TableFile tableFile, String tableName, TableDesc tableDesc) {
        DbTable dbTable = new DbTable(tableName, tableFile, tableDesc);
        tableId2Table.put(tableFile.getTableId(), dbTable);
        tableName2Table.put(tableName, dbTable);
    }

    /**
     * add table
     *
     * @param tableFile table file
     * @param tableName table name
     */
    public void addTable(TableFile tableFile, String tableName) {
        TableDesc tableDesc = tableFile.getTableDesc();
        if (tableDesc == null) {
            throw new DbException("table file's table desc must not be null");
        }

        DbTable dbTable = new DbTable(tableName, tableFile, tableDesc);
        tableId2Table.put(tableFile.getTableId(), dbTable);
        tableName2Table.put(tableName, dbTable);
    }

    public DbTable getDbTableById(int tableId) {
        DbTable dbTable = tableId2Table.get(tableId);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
    }

    public DbTable getDbTableByName(String name) {
        DbTable dbTable = tableName2Table.get(name);
        if (dbTable == null) {
            throw new DbException("table not exist");
        }
        return dbTable;
    }


    @VisibleForTest
    public static void resetWithFile(TableFile tableFile, String tableName) {
        DataBase dataBase = new DataBase();
        dataBase.addTable(tableFile, tableName);
        singleton.set(dataBase);
    }

    @VisibleForTest
    public static void reset() {
        DataBase dataBase = new DataBase();
        singleton.set(dataBase);
    }
}