package com.slowpoke.model.row;

import com.slowpoke.model.page.PageID;

import java.io.Serializable;

/**
 * key id and its page
 *
 * @author zhangjw
 * @version 1.0
 */
public class RowID implements Serializable {
    private static final long serialVersionUID = -6571929580121756569L;

    /**
     * pageID
     */
    private final PageID pageID;

    /**
     * slot index
     */
    private final int slotIndex;

    public RowID(PageID pageID, int slotIndex) {
        this.pageID = pageID;
        this.slotIndex = slotIndex;
    }

    public PageID getPageID() {
        return pageID;
    }

    public int getSlotIndex() {
        return slotIndex;
    }
}
