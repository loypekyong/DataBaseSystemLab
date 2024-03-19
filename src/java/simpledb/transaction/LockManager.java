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
    Map<PageId, MyLock> pgIdLock;
    Map<TransactionId, Set<TransactionId>> dependancyGraph;
    Map<TransactionId, Set<PageId>> tidpagesheld;

    public LockManager() {
        pgIdLock = new HashMap<PageId, MyLock>();
        dependancyGraph = new HashMap<TransactionId, Set<TransactionId>>();
        tidpagesheld = new HashMap<TransactionId, Set<PageId>>();
    }

    public void acquireReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        MyLock lock;
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.heldBy(tid)) return;
            if (!lock.holders().isEmpty() && lock.isExclusive()) {
                dependancyGraph.put(tid, lock.holders());
                if (hasDeadLock(tid)) {
                    dependancyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.readLock(tid);
        synchronized (this) {
            dependancyGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        MyLock lock;
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.isExclusive() && lock.heldBy(tid))
                return;
            if (!lock.holders().isEmpty()){
                dependancyGraph.put(tid, lock.holders());
                if (hasDeadLock(tid)) {
                    dependancyGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.writeLock(tid);
        synchronized (this) {
            dependancyGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!pgIdLock.containsKey(pid)) return;
        MyLock lock = pgIdLock.get(pid);
        lock.unlock(tid);
        tidpagesheld.get(tid).remove(pid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (!tidpagesheld.containsKey(tid)) return;
        Set<PageId> pages = tidpagesheld.get(tid);
        for (Object pageId: pages.toArray()) {
            releaseLock(tid, ((PageId) pageId));
        }
        tidpagesheld.remove(tid);
    }

    private MyLock getOrCreateLock(PageId pageId) {
        if (!pgIdLock.containsKey(pageId))
            pgIdLock.put(pageId, new MyLock());
        return pgIdLock.get(pageId);
    }

    private Set<PageId> getOrCreatePagesHeld(TransactionId tid) {
        if (!tidpagesheld.containsKey(tid))
            tidpagesheld.put(tid, new HashSet<PageId>());
        return tidpagesheld.get(tid);
    }

    private boolean hasDeadLock(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> q = new LinkedList<TransactionId>();
        visited.add(tid);
        q.offer(tid);
        while (!q.isEmpty()) {
            TransactionId head = q.poll();
            if (!dependancyGraph.containsKey(head)) continue;
            for (TransactionId adj: dependancyGraph.get(head)) {
                if (adj.equals(head)) continue;

                if (!visited.contains(adj)) {
                    visited.add(adj);
                    q.offer(adj);
                } else {
                    // Deadlock detected!
                    return true;
                }
            }
        }
        return false;
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return tidpagesheld.containsKey(tid)
                && tidpagesheld.get(tid).contains(pid);
    }

    public Set<PageId> getPagesHeldBy(TransactionId tid) {
        if (tidpagesheld.containsKey(tid))
            return tidpagesheld.get(tid);
        return null;
    }
}

class MyLock {
    Set<TransactionId> holders;
    Map<TransactionId, Boolean> acquirers;
    boolean exclusive;
    private int readNum;
    private int writeNum;

    public MyLock() {
        holders = new HashSet<TransactionId>();
        acquirers = new HashMap<TransactionId, Boolean>();
        exclusive = false;
        readNum = 0;
        writeNum = 0;
    }

    public void readLock(TransactionId tid) {
        if (holders.contains(tid) && !exclusive) return;
        acquirers.put(tid, false);
        synchronized (this) {
            try {
                while (writeNum != 0) this.wait();
                ++readNum;
                holders.add(tid);
                exclusive = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    public void writeLock(TransactionId tid) {
        if (holders.contains(tid) && exclusive) return;
        if (acquirers.containsKey(tid) && acquirers.get(tid)) return;
        acquirers.put(tid, true);
        synchronized (this) {
            try {
                if (holders.contains(tid)) {
                    while (holders.size() > 1) {
                        this.wait();
                    }
                    readUnlockWithoutNotifyingAll(tid);
                }
                while (readNum != 0 || writeNum != 0) this.wait();
                ++writeNum;
                holders.add(tid);
                exclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        acquirers.remove(tid);
    }

    private void readUnlockWithoutNotifyingAll(TransactionId tid) {
        if (!holders.contains(tid)) return;
        synchronized (this) {
            --readNum;
            holders.remove(tid);
        }
    }

    public void readUnlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        synchronized (this) {
            --readNum;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void writeUnlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        if (!exclusive) return;
        synchronized (this) {
            --writeNum;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void unlock(TransactionId tid) {
        if (!exclusive)
            readUnlock(tid);
        else writeUnlock(tid);
    }

    public Set<TransactionId> holders() {
        return holders;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public Set<TransactionId> acquirers() {
        return acquirers.keySet();
    }

    public boolean heldBy(TransactionId tid) {
        return holders().contains(tid);
    }
}