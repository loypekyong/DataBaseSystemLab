package simpledb.storage;

import simpledb.transaction.TransactionId;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.Database;



public class HeapFileIterator implements DbFileIterator {

    private TransactionId tid;
    private HeapFile heapFile;
    private int currentPageNo;
    private Iterator<Tuple> currentTupleIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        currentPageNo = 0;
        currentTupleIterator = getPageTuples(currentPageNo);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (currentTupleIterator == null) {
            return false;
        }

        if (currentTupleIterator.hasNext()) {
            return true;
        }

        if (currentPageNo < heapFile.numPages() - 1) {
            currentPageNo++;
            currentTupleIterator = getPageTuples(currentPageNo);
            return currentTupleIterator.hasNext();
        }

        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return currentTupleIterator.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        currentTupleIterator = null;
    }

    private Iterator<Tuple> getPageTuples(int pageNo) throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(heapFile.getId(), pageNo);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        return page.iterator();
    }
}