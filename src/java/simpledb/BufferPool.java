package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    ConcurrentHashMap<PageId, Page> m_pgHash;
    LinkedHashMap<PageId, Boolean> evictMap;
    int m_limit;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	m_pgHash = new ConcurrentHashMap<PageId, Page>();
    	m_limit = numPages;
    	evictMap = new LinkedHashMap<PageId, Boolean>(numPages/2,0.75f,true);
    	PLock.reset();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	PLock.acquireLock(tid, pid, perm);

    	if (m_pgHash.size() >= m_limit && !m_pgHash.containsKey(pid))
    		evictPage();
    	
    	if (evictMap.containsKey(pid))
    		evictMap.get(pid);
    	else
    		evictMap.put(pid, false);
    	
    	
    	if (m_pgHash.containsKey(pid))
    		return m_pgHash.get(pid);


    	Page pg;
    	DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
    	
    	pg = file.readPage(pid);
    	m_pgHash.put(pid, pg);
        return pg;
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    	PLock.releaseLock(tid, pid);
    }

    /** Releases all locks associated with this transaction **/
    public void releaseAllPageLocks(TransactionId tid){
    	PLock.releaseByTrans(tid);    	
    }
    
    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return PLock.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    	if (commit){
    		flushPages(tid);
    	} else {
    		//abort
    		//revert any changes made by the transaction by restoring the page to its on-disk state
    		revertPages(tid);
    	}
    	PLock.releaseByTrans(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	DbFile f = Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> pgs = f.insertTuple(tid, t);
    	for (int i=0; i<pgs.size(); i++){
    		Page p = pgs.get(i);
        	p.markDirty(true, tid);
        	//Required?: (since pgs could contain a new page).
        	m_pgHash.put(p.getId(), p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	int tblId = t.m_rid.pid.getTableId();
    	DbFile f = Database.getCatalog().getDbFile(tblId);
    	Page pg = f.deleteTuple(tid, t);
        pg.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
    	Iterator<PageId> it = m_pgHash.keySet().iterator();
    	while (it.hasNext()){
    		flushPage(it.next());    		
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    	if(!m_pgHash.containsKey(pid)){
    		//Todo? Change into exception?
    		System.out.println("Tried to discard a page that doesn't exist in the hash.");
    		assert(true==false);
    	}
    	if(!evictMap.containsKey(pid)){
    		//Todo? Change into exception?
    		System.out.println("Tried to discard a page that doesn't exist in the hash.");
    		assert(true==false);
    	}
    	m_pgHash.remove(pid);
    	evictMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	Page p = m_pgHash.get(pid);
    	//figure out while file the page is in.
    	DbFile f = Database.getCatalog().getDbFile(pid.getTableId());
    	f.writePage(p);
    	p.markDirty(false, null);	
    }

    /** Revert all pages of the specified transaction to oldData **/
    public synchronized void revertPages(TransactionId tid) throws IOException {
    	Iterator<Page> it = m_pgHash.values().iterator();
    	while (it.hasNext()){
    		Page p = it.next();
    		TransactionId dirtier = p.isDirty();
    		if (dirtier != null && tid.equals(dirtier)){
    			PageId pid = p.getId();
                Page oldPage = p.getBeforeImage();
                m_pgHash.put(pid, oldPage);
    			p.markDirty(false, null);
    		}
    	}
    }
    
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	Iterator<Page> it = m_pgHash.values().iterator();
    	while (it.hasNext()){
    		Page p = it.next();
    		TransactionId dirtier = p.isDirty();
    		if (dirtier != null && tid.equals(dirtier)){
    			flushPage(p.getId());
    			p.setBeforeImage();
    			p.markDirty(false, null);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    	PageId pid;
    	PageId pidToFlush = null;
    	
    	
//    	Iterator<PageId> it2 = m_pgHash.keySet().iterator();
    	Iterator<PageId> it = evictMap.keySet().iterator();
//    	while (it.hasNext())
//    		System.out.println("1: " + it.next().pageNumber());
//    	while (it.hasNext())
//    		System.out.println("2: " + it2.next().pageNumber());
    	
    	
    	while(it.hasNext()){
    		pid = it.next();
    		Page p = m_pgHash.get(pid);
    		// Page.isDirty() returns null if clean
    		if (p.isDirty() == null){
    			pidToFlush = pid;
    			break;
    		}
    	}
    	
    	
    	//no longer flush it, because it is not dirty
    	/*
    	try {
    		flushPage(pidToFlush);
    	} catch (IOException e){
    		throw new DbException("IOException" + e.getMessage());
    	}
    	*/
    	
    	//if all of the pages in evictMap were dirty
    	if (pidToFlush == null)
    		throw new DbException("No clean pages found, when evicting in BufferPool.java.");
    	
    	discardPage(pidToFlush);
    }
}
