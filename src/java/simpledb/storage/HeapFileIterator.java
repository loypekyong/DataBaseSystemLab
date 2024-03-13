package simpledb.storage;

import simpledb.transaction.TransactionId;
import java.util.Iterator;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.Database;



public class HeapFileIterator extends AbstractDbFileIterator{

    private TransactionId tid;
    private int tableId;
    private int numPages;
    private int currentPage;
    private Iterator<Tuple> tupleIterator;

    public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
        this.currentPage = 0;
    }

    public void open() throws DbException, TransactionAbortedException {
        HeapPageId pid = new HeapPageId(tableId, currentPage);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        tupleIterator = page.iterator();
    }

    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            return null;
        }
        if (tupleIterator.hasNext()) {
            return tupleIterator.next();
        } else {
            currentPage++;
            if (currentPage < numPages) {
                HeapPageId pid = new HeapPageId(tableId, currentPage);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                tupleIterator = page.iterator();
                if (tupleIterator.hasNext()) {
                    return tupleIterator.next();
                }
            }
        }
        return null;
    }

    public void close() {
        super.close();
        tupleIterator = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
}