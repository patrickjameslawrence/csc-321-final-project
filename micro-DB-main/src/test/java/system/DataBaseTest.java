package system;

import base.TestBase;
import com.slowpoke.bufferpool.BufferPool;
import com.slowpoke.connection.Connection;
import com.slowpoke.model.DataBase;
import com.slowpoke.model.table.DbTable;
import com.slowpoke.model.row.Row;
import com.slowpoke.model.table.TableDesc;
import com.slowpoke.model.table.tablefile.HeapTableFile;
import com.slowpoke.model.table.tablefile.TableFile;
import com.slowpoke.model.field.FieldType;
import com.slowpoke.model.field.IntField;
import com.slowpoke.model.page.heap.HeapPage;
import com.slowpoke.model.page.heap.HeapPageID;
import com.slowpoke.transaction.Lock;
import com.slowpoke.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * test
 *
 * @author Dr. Chen
 * @version 1.0
 */
public class DataBaseTest extends TestBase {

    public DataBase dataBase;
    private BufferPool bufferPool;

    @Before
    public void initDataBase() {
        DataBase dataBase = DataBase.getInstance();
        // create database file
        String fileName = UUID.randomUUID().toString();
        List<TableDesc.Attribute> attributes = Arrays.asList(new TableDesc.Attribute("f1", FieldType.INT));
        TableDesc tableDesc = new TableDesc(attributes);
        File file = new File(fileName);
        file.deleteOnExit();
        TableFile tableFile = new HeapTableFile(file, tableDesc);

        // tableDesc
        dataBase.addTable(tableFile, "t_person", tableDesc);

        this.dataBase = dataBase;
        this.bufferPool = DataBase.getBufferPool();

    }
    @Test
    public void testBufferPool() throws IOException {
        Transaction transaction = new Transaction(Lock.LockType.XLock);
        transaction.start();
        Connection.passingTransaction(transaction);

        DbTable tablePerson = this.dataBase.getDbTableByName("t_person");
        Row row = new Row(tablePerson.getTableDesc());
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(1, existPageCount);
        }

        // page 2
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(2, existPageCount);
        }

        // page 3
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(3, existPageCount);
        }

        // page 4
        for (int i = 0; i < 819; i++) {
            row.setField(0, new IntField(i));
            tablePerson.insertRow(row);
            int existPageCount = tablePerson.getTableFile().getExistPageCount();
            Assert.assertEquals(4, existPageCount);
        }
        //HashMap<PageID, Page> pages = bufferPool.g
        transaction.commit();
    }


// JUnit
}
