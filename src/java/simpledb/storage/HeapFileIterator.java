package simpledb.storage;

import simpledb.transaction.TransactionId;
import java.util.Iterator;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.Database;



public class HeapFileIterator extends AbstractDbFileIterator{

    private HeapFile heapFile;
    private TransactionId tid;

    private int pageIndex;

    private final int tableId;
    private final int pageNum;
    private Iterator<Tuple> iterator;
    public HeapFileIterator(int tableId, TransactionId tid, int pageNum) {
        this.tid = tid;
        this.tableId = tableId;
        this.pageNum = pageNum;
        pageIndex = 0;
    }

    public void open() throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(tableId, pageIndex);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        iterator = page.iterator();
    }

    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (iterator == null)
            return null;

        if (iterator.hasNext())
            return iterator.next();
        if (pageIndex < pageNum - 1) {
            pageIndex++;
            open();
            return readNext();
        }

        return null;
    }

    public void close() {
        super.close();
        iterator = null;
        pageIndex = 0;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }


}