package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
// import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//import reentrantreadwritelock
import java.util.concurrent.locks.ReentrantReadWriteLock;



/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    public static int numPages;

    private final LockManager lockManager = new LockManager();

    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    
    private LinkedHashMap<PageId, Page> pool; //for LRU

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        BufferPool.numPages = numPages;
        // this.lockManager = new LockManager();
        this.pool = new LinkedHashMap<PageId, Page>(16, 0.75f, true) { // access order set to true for LRU
            @Override
            protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
                return size() > numPages; // ensures LRU eviction based on the size constraint
            }
        };
        }
        

    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     * 
     * 
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
  
        /**
         * The BufferPool should store up to `numPages` pages. For this lab, if more than `numPages` requests are made for different pages, then instead of implementing an eviction policy, you may throw a DbException.
         */


         if (perm == Permissions.READ_WRITE) {
            this.lockManager.acquireWriteLock(tid, pid);
        } else if (perm == Permissions.READ_ONLY) {
            this.lockManager.acquireReadLock(tid, pid);
        } else {
            throw new DbException("Invalid permission requested.");
        }

            // If the page is already in the pool
            if (pool.containsKey(pid)) {
                Page page = this.pool.get(pid);
                this.pool.remove(pid);
                this.pool.put(pid, page);
                return page;
            }

            // If the page is not in the pool, add it to the pool
            // If there is insufficient space in the buffer pool, a page should be evicted and the new page should be added in its place.
            if (pool.size() >= numPages) {
                evictPage();
            }

            // If the page is not in the pool, add it to the pool
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            pool.put(pid, page);

            return page;
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);

    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> Arr = (ArrayList<Page>) file.insertTuple(tid, t);
        for (Page pg : Arr) {
            pg.markDirty(true, tid);
            if (!this.pool.containsKey(pg.getId()) && this.pool.size() >= this.numPages) {
                this.evictPage();
            }
            this.pool.remove(pg.getId());
            this.pool.put(pg.getId(), pg); // id assigned
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId() == null) {
            System.out.printf("Null: %s%n", t.toString());
        }
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> pages = (ArrayList<Page>) file.deleteTuple(tid, t);

        for (Page p: pages) {
            p.markDirty(true, tid);
            if (!this.pool.containsKey(p.getId()) && this.pool.size() >= this.numPages) {
                this.evictPage();
            }
            this.pool.remove(p.getId());
            // Assign id to the page
            this.pool.put(p.getId(), p);
        }



    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        List<PageId> pageIds = new ArrayList<>(this.pool.keySet());


        for (PageId pid : pageIds) {
            flushPage(pid); // flushPage call
        }


    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pool.remove(pid); //remove w/o flushing to disk
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException { //write dirty pages to disk
        // some code goes here
        // not necessary for lab1
        Page page = this.pool.get(pid);

        if (this.pool.containsKey(pid)) {
            TransactionId dirty = page.isDirty();
            if (dirty != null) {
                Database.getLogFile().logWrite(dirty, page.getBeforeImage(), page);
                Database.getLogFile().force();
                HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                heapFile.writePage(page);


                page.markDirty(false, null);
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.s
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<PageId> pageIdIterator = this.pool.keySet().iterator();

        while (pageIdIterator.hasNext()) {
            PageId pid = pageIdIterator.next();
            Page page = this.pool.get(pid);

            // If the page is not dirty, flush it and remove it from the pool
            if (page.isDirty() == null) {
                try {
                    this.flushPage(pid);
                    pageIdIterator.remove();
                    return;
                } catch (IOException e) {
                    throw new DbException("Page could not be flushed");
                }
            }
        }

        // If all pages are dirty, throw a DbException
        throw new DbException("All pages in the buffer pool are dirty.");
    }

}
