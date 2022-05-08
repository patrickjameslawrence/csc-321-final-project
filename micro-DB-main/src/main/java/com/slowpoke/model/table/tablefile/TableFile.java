package com.slowpoke.model.table.tablefile;

import com.slowpoke.model.row.Row;
import com.slowpoke.model.table.TableDesc;
import com.slowpoke.model.page.Page;
import com.slowpoke.model.page.PageID;

import java.io.IOException;

/**
 * table file
 *
 * @author zhangjw
 * @version 1.0
 */
public interface TableFile {


    /**
     * get table structure
     */
    TableDesc getTableDesc();

    /**
     * read page from disk
     *
     * @param pageID pageID
     * @return page
     */
    Page readPageFromDisk(PageID pageID);

    /**
     * write page to disk
     */
    void writePageToDisk(Page page);

    /**
     * return table id
     */
    int getTableId();

    /**
     * pages in file
     */
    int getExistPageCount();

    void insertRow(Row row) throws IOException;

    void deleteRow(Row row) throws IOException;

    ITableFileIterator getIterator();
}
