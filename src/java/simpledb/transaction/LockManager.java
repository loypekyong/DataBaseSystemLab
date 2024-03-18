package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    
    ConcurrentHashMap<PageId, List<TransactionId>> pageLocks;
    ConcurrentHashMap<TransactionId, List<PageId>> transactionLocks;
    ConcurrentHashMap<TransactionId, List<PageId>> transactionWaiting;
    
    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
        transactionLocks = new ConcurrentHashMap<>();
        transactionWaiting = new ConcurrentHashMap<>();
    }

    public void acquireReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new ArrayList<>());
        }
        if (!transactionLocks.containsKey(tid)) {
            transactionLocks.put(tid, new ArrayList<>());
        }
        if (!transactionWaiting.containsKey(tid)) {
            transactionWaiting.put(tid, new ArrayList<>());
        }
        
        if (pageLocks.get(pid).contains(tid)) {
            return;
        }
        
        if (transactionLocks.get(tid).contains(pid)) {
            return;
        }
        
        if (pageLocks.get(pid).size() == 0) {
            pageLocks.get(pid).add(tid);
            transactionLocks.get(tid).add(pid);
            return;
        }
        
        if (pageLocks.get(pid).size() == 1 && pageLocks.get(pid).get(0).equals(tid)) {
            return;
        }
        
        if (pageLocks.get(pid).size() == 1 && pageLocks.get(pid).get(0) != tid) {
            if (transactionWaiting.get(pageLocks.get(pid).get(0)).contains(pid)) {
                throw new TransactionAbortedException();
            }
            transactionWaiting.get(pageLocks.get(pid).get(0)).add(pid);
            return;
        }
        
        if (pageLocks.get(pid).size() > 1) {
            if (pageLocks.get(pid).contains(tid)) {
                return;
            }
            if (transactionWaiting.get(pageLocks.get(pid).get(0)).contains(pid)) {
                throw new TransactionAbortedException();
            }
            transactionWaiting.get(pageLocks.get(pid).get(0)).add(pid);
            return;
        }

    }

    public void acquireWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new ArrayList<>());
        }
        if (!transactionLocks.containsKey(tid)) {
            transactionLocks.put(tid, new ArrayList<>());
        }
        if (!transactionWaiting.containsKey(tid)) {
            transactionWaiting.put(tid, new ArrayList<>());
        }
        
        if (pageLocks.get(pid).contains(tid)) {
            return;
        }
        
        if (transactionLocks.get(tid).contains(pid)) {
            return;
        }
        
        if (pageLocks.get(pid).size() == 0) {
            pageLocks.get(pid).add(tid);
            transactionLocks.get(tid).add(pid);
            return;
        }
        
        if (pageLocks.get(pid).size() == 1 && pageLocks.get(pid).get(0).equals(tid)) {
            return;
        }
        
        if (pageLocks.get(pid).size() == 1 && pageLocks.get(pid).get(0) != tid) {
            if (transactionWaiting.get(pageLocks.get(pid).get(0)).contains(pid)) {
                throw new TransactionAbortedException();
            }
            transactionWaiting.get(pageLocks.get(pid).get(0)).add(pid);
            return;
        }
        
        if (pageLocks.get(pid).size() > 1) {
            if (pageLocks.get(pid).contains(tid)) {
                return;
            }
            if (transactionWaiting.get(pageLocks.get(pid).get(0)).contains(pid)) {
                throw new TransactionAbortedException();
            }
            transactionWaiting.get(pageLocks.get(pid).get(0)).add(pid);
            return;
        }
        
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        
        if (!pageLocks.containsKey(pid)) {
            return;
        }
        if (!transactionLocks.containsKey(tid)) {
            return;
        }
        if (!transactionWaiting.containsKey(tid)) {
            return;
        }
        
        if (pageLocks.get(pid).contains(tid)) {
            pageLocks.get(pid).remove(tid);
        }
        
        if (transactionLocks.get(tid).contains(pid)) {
            transactionLocks.get(tid).remove(pid);
        }
        
        if (transactionWaiting.get(tid).contains(pid)) {
            transactionWaiting.get(tid).remove(pid);
        }
        
        if (pageLocks.get(pid).size() == 0) {
            pageLocks.remove(pid);
        }
        
        if (transactionLocks.get(tid).size() == 0) {
            transactionLocks.remove(tid);
        }
        
        if (transactionWaiting.get(tid).size() == 0) {
            transactionWaiting.remove(tid);
        }
        
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        
        if (!pageLocks.containsKey(pid)) {
            return false;
        }
        if (!transactionLocks.containsKey(tid)) {
            return false;
        }
        if (!transactionWaiting.containsKey(tid)) {
            return false;
        }
        
        if (pageLocks.get(pid).contains(tid)) {
            return true;
        }
        
        if (transactionLocks.get(tid).contains(pid)) {
            return true;
        }
        
        if (transactionWaiting.get(tid).contains(pid)) {
            return true;
        }
        
        return false;
    }

}