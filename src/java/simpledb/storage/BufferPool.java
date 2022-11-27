package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final int numPages;

    private final ConcurrentHashMap<PageId, LinkNode> pageStore;

    private final LockManager manager;


    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;


    // lab3 page evict.
    public static  class LinkNode {
        PageId pageId;
        Page page;
        LinkNode prev;
        LinkNode next;
        public LinkNode(PageId pageId, Page page) {
            this.page = page;
            this.pageId = pageId;
        }
    }

    // define head and tail as two head,but they are not store data.
    LinkNode head;
    LinkNode tail;


    public void addToHead(LinkNode node) {
        node.prev = head;
        node.next = head.next;
        node.next.prev = node;
        head.next = node;
    }

    public void remove(LinkNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public void moveToHead(LinkNode node) {
        remove(node);
        addToHead(node);
    }


    // evict one page from bufferPool
    public LinkNode removeTail() {
        LinkNode node = tail.prev;
        remove(node);
        return node;
    }


    // lab4 TransactionId
    public class PageLock {
        public static final int SHARE = 0;
        public static final int EXCLUSIVE = 1;

        private int lockType;

        private TransactionId tid;

        public PageLock(int type, TransactionId tid) {
            this.lockType = type;
            this.tid = tid;
        }

        public PageLock() {

        }

        public int getLockType() {
            return lockType;
        }

        public TransactionId getTid() {
            return tid;
        }

        public void setLockType(int lockType) {
            this.lockType = lockType;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }
    }


    public class LockManager {
        private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> locks;

        public LockManager() {
            locks = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType) {
            // get the locks on the page.
            if(!locks.containsKey(pid)){
                PageLock pageLock = new PageLock(lockType,tid);
                ConcurrentHashMap<TransactionId, PageLock> map = new ConcurrentHashMap<>();
                map.put(tid, pageLock);
                locks.put(pid, map);
                return true;
            }

            ConcurrentHashMap<TransactionId, PageLock> pageMap = locks.get(pid);
            if(pageMap.containsKey(tid)) {
                PageLock pageLock = pageMap.get(tid);
                // already have a share lock.
                if(pageLock.getLockType() == PageLock.SHARE) {
                    if(lockType == PageLock.SHARE) {
                        return true;
                    }else if(lockType == PageLock.EXCLUSIVE){
                        // if transaction t is the only transaction holding a shared lock on an object o,t may upgrade its lock on o to an exclusive lock.
                        if(pageMap.size() == 1) {
                            pageLock.setLockType(PageLock.EXCLUSIVE);
                            pageMap.put(tid, pageLock);
                            locks.put(pid, pageMap);
                            return true;
                        }
                        return false;
                    }
                }

                return pageLock.getLockType() == PageLock.EXCLUSIVE;
            }else{
                // the page have other transaction lock.


                // the page's size > 1 confirm there is a shore lock.
                if(pageMap.size()>1){
                    if(lockType == PageLock.SHARE) {
                        PageLock pageLock = new PageLock(lockType, tid);
                        pageMap.put(tid, pageLock);
                        locks.put(pid, pageMap);
                        return true;
                    }else{
                        return false;
                    }
                }else if(pageMap.size()==1){
                    // the page's size == 1 confirm the lock is a share lock or exclusive lock.
                    PageLock pageLock = new PageLock();
                    for(PageLock item : pageMap.values()) {
                        pageLock = item;
                    }

                    // hold a share lock.
                    if(pageLock.getLockType() == PageLock.SHARE){
                        if(lockType == PageLock.SHARE){
                            PageLock newPageLock = new PageLock(PageLock.SHARE, tid);
                            pageMap.put(tid, newPageLock);
                            locks.put(pid, pageMap);
                            return true;
                        }else if(lockType == PageLock.EXCLUSIVE){
                            return false;
                        }
                    }else if(pageLock.getLockType() == PageLock.EXCLUSIVE){
                        // hold a exclusive lock.
                        return false;
                    }
                }
            }
            return false;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            if(isHoldLock(tid, pid)){
                ConcurrentHashMap<TransactionId, PageLock> map = locks.get(pid);
                map.remove(tid);
                if(map.size()==0){
                    locks.remove(pid);
                }
            }
        }

        public synchronized boolean isHoldLock(TransactionId tid, PageId pageId) {
            if(!locks.containsKey(pageId)) {
                return false;
            }else{
                ConcurrentHashMap<TransactionId, PageLock> map = locks.get(pageId);
                return map.containsKey(tid);
            }
        }

        public synchronized void completeTransaction(TransactionId tid) {
            for(PageId pid : locks.keySet()) {
                releaseLock(tid, pid);
            }
        }
    }




    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<PageId, LinkNode>();
        head = new LinkNode(new HeapPageId(-1,-1), null);
        tail = new LinkNode(new HeapPageId(-1,-1), null);

        head.next = tail;
        tail.prev = head;

        manager = new LockManager();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        int getType = (perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE);
        long start = System.currentTimeMillis();
        while(true) {
            if(!manager.acquireLock(pid, tid, getType)) {
                long now = System.currentTimeMillis();
                if(now - start > 500) {
                    throw new TransactionAbortedException();
                }
            }else{
                break;
            }
        }

        // successful get a lock.
        // TODO: some code goes here
        if(!pageStore.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if(pageStore.size() >= numPages) {
                evictPage();
            }
            LinkNode node = new LinkNode(pid, page);
            pageStore.put(pid, node);
            addToHead(node);
        }
        moveToHead(pageStore.get(pid));
        return pageStore.get(pid).page;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        manager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        manager.completeTransaction(tid);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return manager.isHoldLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        updateBufferPool(pages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = file.deleteTuple(tid, t);
        updateBufferPool(pages, tid);
    }


    public void updateBufferPool(List<Page> pages, TransactionId tid) throws DbException {
        for(Page page: pages) {
            page.markDirty(true,tid);
            if(pageStore.size() > numPages){
                evictPage();
            }
            LinkNode node = pageStore.get(page.getId());
            node.page = page;
            pageStore.put(page.getId(), node);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        for(PageId pageId : pageStore.keySet()) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        Page page = pageStore.get(pid).page;
        if(page.isDirty()!=null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        LinkNode node = removeTail();
        try {
            // evict this page need to write the page into the disk.
            flushPage(node.pageId);
        }catch (IOException e) {
            e.printStackTrace();
        }

        pageStore.remove(node.pageId);
    }
}
