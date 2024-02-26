package simpledb.storage; 
 
import simpledb.transaction.TransactionAbortedException; 
import simpledb.transaction.TransactionId; 
import simpledb.common.DbException; 
import java.util.Iterator; 
import java.util.NoSuchElementException; 
import simpledb.common.Database; 
import simpledb.common.Permissions; 
 
public class HeapFileIterator implements DbFileIterator { 
 
    private final TransactionId tid; 
    private final HeapFile heapFile; 
    private Iterator<Tuple> tupleIterator; 
    private int currentPageNumber; 
 
    public HeapFileIterator(TransactionId tid, HeapFile heapFile) { 
        this.tid = tid; 
        this.heapFile = heapFile; 
        this.tupleIterator = null; 
        this.currentPageNumber = 0; 
    } 
 
    /* loads the iterator for the first page into the tupleIterator */ 
    @Override 
    public void open() throws DbException, TransactionAbortedException { 
        currentPageNumber = 0; 
        HeapPageId heapPageId = new HeapPageId(heapFile.getId(), currentPageNumber); 
     
        // Load the first page into the buffer pool 
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY); 
     
        tupleIterator = heapPage.iterator(); 
    } 
 
    /* 
     * If the current page has more tuples, return true. Otherwise, move to the next page and return true if the next page has tuples. 
     */ 
    @Override 
    public boolean hasNext() throws DbException, TransactionAbortedException { 
        if (tupleIterator == null) { 
            return false; // No iterator available 
        } 
     
        // If there is a next tuple in the current page, return true 
        if (tupleIterator.hasNext()) { 
            return true; 
        } 
     
        // Move to the next page if available 
        currentPageNumber++; 
        if (currentPageNumber < heapFile.numPages()) { 
            HeapPageId heapPageId = new HeapPageId(heapFile.getId(), currentPageNumber); 
     
            // Load the page into the buffer pool 
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY); 
     
            tupleIterator = heapPage.iterator(); 
     
            return tupleIterator.hasNext(); // Return true if the page has tuples 
        } 
     
        return false; // No more pages with tuples 
    } 
 
    /* 
     * returns the next tuple into the iteration 
     */ 
    @Override 
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException { 
        if (!hasNext()) { 
            throw new NoSuchElementException("No more tuples in the file"); 
        } 
        return tupleIterator.next(); 
    } 
 
    /* 
     * close and reopens the iterator 
     */ 
    @Override 
    public void rewind() throws DbException, TransactionAbortedException { 
        close(); 
        open(); 
    } 
 
    /* 
     *  
     */ 
    @Override 
    public void close() { 
        tupleIterator = null; 
        currentPageNumber = 0; 
    } 
}