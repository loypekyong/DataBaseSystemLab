package simpledb.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

class MyLock {

    TransactionId tid;
    PageId pid;
    boolean exclusive;

    public MyLock(TransactionId tid, PageId pid, boolean exclusive) {
        this.tid = tid;
        this.pid = pid;
        this.exclusive = exclusive;
    }

    public boolean equals(Object o) {
        if (!(o instanceof MyLock))
            return false;
        MyLock other = (MyLock) o;
        return this.tid.equals(other.tid) && this.pid.equals(other.pid) && this.exclusive == other.exclusive;
    }

    @Override
    public int hashCode() {
        return (tid != null ? tid.hashCode() : 0) + pid.hashCode() + (exclusive ? 1 : 0);
    }
}

public class LockManager {
    private HashMap<PageId, ArrayList<MyLock>> pgIdLock;
    private HashMap<TransactionId, HashSet<PageId>> transIdtoPgId;
    private DependencyGraph dependencyGraph;

    public LockManager() {
        pgIdLock = new HashMap<PageId, ArrayList<MyLock>>();
        transIdtoPgId = new HashMap<TransactionId, HashSet<PageId>>();
        dependencyGraph = new DependencyGraph();
    }

    public synchronized boolean pageLock(TransactionId tid, PageId pid, boolean exclusiveLock)
            throws TransactionAbortedException {
        Permissions existingPermissions = getLockHeldType(tid, pid);
        if (existingPermissions != null)
            if (existingPermissions == Permissions.READ_WRITE
                    || (!exclusiveLock && (existingPermissions == Permissions.READ_ONLY)))
                return true;

        boolean canAquire = false;
        ArrayList<MyLock> otherLocks = null;
        if (pgIdLock.containsKey(pid)) {
            otherLocks = pgIdLock.get(pid);
            if (otherLocks.isEmpty())
                canAquire = true;
            else {
                if (upgradeLock(tid, pid))
                    return true;
                boolean otherExclusiveLock = false;
                for (MyLock l : otherLocks)
                    if (l.exclusive)
                        otherExclusiveLock = true;
                if (!exclusiveLock && !otherExclusiveLock)
                    canAquire = true;
            }
        } else {
            canAquire = true;
        }

        // if (!canAquire) {
        //     addDependencies(tid, pid, exclusiveLock, otherLocks);
        //     return false;
        // }

        MyLock newLock = new MyLock(tid, pid, exclusiveLock);
        if (!pgIdLock.containsKey(pid))
            pgIdLock.put(pid, new ArrayList<MyLock>());
            pgIdLock.get(pid).add(newLock);

        if (!transIdtoPgId.containsKey(tid))
            transIdtoPgId.put(tid, new HashSet<PageId>());
            transIdtoPgId.get(tid).add(pid);

        return true;
    }

    private Permissions getLockHeldType(TransactionId tid, PageId pid) {
        if (pgIdLock.containsKey(pid)) {
            ArrayList<MyLock> locks = pgIdLock.get(pid);
            for (MyLock l : locks)
                if (l.tid.equals(tid))
                    return l.exclusive ? Permissions.READ_WRITE : Permissions.READ_ONLY;
        }
        return null;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if(!transIdtoPgId.containsKey(tid))
            return false;
        return transIdtoPgId.get(tid).contains(pid);
    }

    public synchronized void pageUnlock(TransactionId tid, PageId pid) {
        if(!pgIdLock.containsKey(pid))
            return;

        Iterator<MyLock> pageLockIterator = pgIdLock.get(pid).iterator();
        while(pageLockIterator.hasNext()) {
            MyLock l = pageLockIterator.next();
            if(l.tid.equals(tid))
                pageLockIterator.remove();
        }

        // dependencyGraph.removeDependencies(tid, pid);

        if(transIdtoPgId.containsKey(tid))
            transIdtoPgId.get(tid).remove(pid);
    }

    public void unlockAllPages(TransactionId tid) {
        HashSet<PageId> pageIds;

        synchronized (this) {
            if(!transIdtoPgId.containsKey(tid))
                return;

            pageIds = transIdtoPgId.get(tid);
            transIdtoPgId.remove(tid);
        }

        for(PageId pid : pageIds)
            pageUnlock(tid, pid);
    }

    public synchronized Set<PageId> getPagesLockedByTx(TransactionId tid) {
        return transIdtoPgId.get(tid);
    }

    private boolean upgradeLock(TransactionId tid, PageId pid) {
        if (pgIdLock.containsKey(pid)) {
            ArrayList<MyLock> pageLocks = pgIdLock.get(pid);
            if (pageLocks.size() == 1) {
                MyLock onlyLock = pageLocks.iterator().next();
                if (onlyLock.tid.equals(tid)) {
                    onlyLock.exclusive = true;
                    return true;
                }
            }
        }
        return false;
    }

    // private void addDependencies(TransactionId tid, PageId pid, boolean exclusiveLock, ArrayList<MyLock> otherLocks) throws TransactionAbortedException {
    //     // if requested lock is exclusive, we must wait for all other locks to be released
    //     if(exclusiveLock) {
    //         for(MyLock l : otherLocks)
    //             dependencyGraph.addDependency(tid, l.tid, pid);
    //     } else {
    //         for(MyLock l : otherLocks)
    //             if(l.exclusive)
    //                 dependencyGraph.addDependency(tid, l.tid, pid);
    //     }
    // }
}