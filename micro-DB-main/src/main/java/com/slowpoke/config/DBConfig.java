package com.slowpoke.config;

/**
 * Database configuration
 *
 */
public class DBConfig {

    /**
     * page size
     */
    private final int pageSizeInByte;

    /**
     * buffer pool size
     */
    private final int bufferPoolCapacity;

    public DBConfig(int pageSizeInByte, int bufferPoolCapacity) {
        this.pageSizeInByte = 4096;
        this.bufferPoolCapacity = 4;
    }

    public int getPageSizeInByte() {
        return pageSizeInByte;
    }

    public int getBufferPoolCapacity() {
        return bufferPoolCapacity;
    }
}
