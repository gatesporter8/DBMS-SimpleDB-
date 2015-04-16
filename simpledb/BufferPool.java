package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


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
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int maxPages;
    
    private final AtomicInteger currentPages;
    
    private final Map<PageId, Page> pageIdToPages;
    
    private final Map<TransactionId, Set<PageId>> transactionsToDirtiedFlushedPages;
    
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	this.maxPages = numPages;
    	this.pageIdToPages = new HashMap<PageId, Page>();
    	this.transactionsToDirtiedFlushedPages = new HashMap<TransactionId, Set<PageId>>();
    	this.lockManager = LockManager.create();
    	currentPages = new AtomicInteger(0);
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
    	BufferPool.pageSize = PAGE_SIZE;
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


    // For Lab 4, make sure that reads from getPage are synchronized()
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	lockManager.acquireLock(tid, pid, perm);
    	
    	if (pageIdToPages.containsKey(pid)) {
    		
    		return pageIdToPages.get(pid);
    	}
    	if (currentPages.get() == maxPages) {
    		
    		evictPage();
    	}
    	
    	int tableId = pid.getTableId();
    	
    	Catalog catalog = Database.getCatalog();
    	
    	DbFile dbFile = catalog.getDatabaseFile(tableId);
    	
    	Page page = dbFile.readPage(pid);
    	
    	pageIdToPages.put(pid, page);
    	
    	currentPages.incrementAndGet();
    	
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
    	lockManager.releasePage(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
    	transactionComplete(tid,true);
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
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
    	if (commit) {
    		
    		Set<PageId> dirtiedFlushedPages = transactionsToDirtiedFlushedPages.get(tid);
    		
    		for (PageId pageId : pageIdToPages.keySet()) {
    		
    			Page page = pageIdToPages.get(pageId);
    		
    			if (tid.equals(page.isDirty())) {
    		
    				flushPage(pageId);
    		
    				page.setBeforeImage();
    		
    			} else if (dirtiedFlushedPages != null && dirtiedFlushedPages.contains(pageId)) {
    		
    				page.setBeforeImage();
    		
    			}
    		
    		}
    		
    	} else {
    		
    		for (PageId pageId : pageIdToPages.keySet()) {
    		
    			Page page = pageIdToPages.get(pageId);
    		
    			if (tid.equals(page.isDirty())) {
    		
    				pageIdToPages.put(pageId, page.getBeforeImage());
    		
    				page.markDirty(false, null);
    		
    			}
    		
    		}
    	
    	}
    	
    	transactionsToDirtiedFlushedPages.remove(tid);
    	
    	lockManager.releasePages(tid);
    	
    	// if commit, flush dirty pages
    	
    	// if no commit, restore dirty pages
    		
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

    // For Lab 4, make sure that inserts are synchronized()
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	// let the specific implementation of the file decide which page to add it
    	// to.
    	ArrayList<Page> dirtypages = file.insertTuple(tid, t);
    	synchronized(this) {
    	for (Page p : dirtypages){
    	p.markDirty(true, tid);
    	//System.out.println("ADDING TUPLE TO PAGE " + p.getId().pageno() + " WITH HASH CODE " + p.getId().hashCode());
    	// if page in pool already, done.
    	if(pageIdToPages.get(p.getId()) != null) {
    	//replace old page with new one in case addTuple returns a new copy of the page
    	pageIdToPages.put(p.getId(), p);
    	}
    	else {
    	// put page in pool
    	if(pageIdToPages.size() >= maxPages)
    	evictPage();
    	pageIdToPages.put(p.getId(), p);
    	}
    	}
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


    // For Lab 4, make sure that deletes are synchronized()
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	
    	ArrayList<Page> dirtypages = file.deleteTuple(tid, t);
    	
    	synchronized(this) {
    	
    		for (Page p : dirtypages){
    	
    			p.markDirty(true, tid);
    	
    			// if page in pool already, done.
    	
    			if(pageIdToPages.get(p.getId()) != null) {
    	
    				//replace old page with new one in case deleteTuple returns a new copy of the page
    	
    				pageIdToPages.put(p.getId(), p);
    	
    			}
    	
    			else {
    	
    				// put page in pool
    	
    				if(pageIdToPages.size() >= maxPages)
    	
    					evictPage();
    	
    				pageIdToPages.put(p.getId(), p);
    	
    			}
    	
    		}
    	} 
    	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	
    	for (PageId pageId : pageIdToPages.keySet()) {
    		
    		flushPage(pageId);
    		
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
    	if (pageIdToPages.containsKey(pid)) {
    		
    		pageIdToPages.remove(pid);
    		
    		currentPages.decrementAndGet();
    		
    	}
    		
    }
    
    private void addDirtiedFlushedPage(TransactionId dirtier, PageId pageId) {
    	
    	if (transactionsToDirtiedFlushedPages.containsKey(dirtier)) {
    	
    		transactionsToDirtiedFlushedPages.get(dirtier).add(pageId);
    	
    	} else {
    	
    		Set<PageId> dirtiedFlushedPages = new HashSet<PageId>();
    	
    		dirtiedFlushedPages.add(pageId);
    	
    		transactionsToDirtiedFlushedPages.put(dirtier, dirtiedFlushedPages);
    	
    	}
    	
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        
    	if (pageIdToPages.containsKey(pid)) {
    		
    		Page page = pageIdToPages.get(pid);
    		
    		TransactionId dirtier = page.isDirty();
    	
    		if (dirtier != null) {
    	
    			addDirtiedFlushedPage(dirtier, pid);
    		
    			Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
    		
    			Database.getLogFile().force();
    	
    			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    	
    			page.markDirty(false, null);
    		
    		}
    		
    	}
    		
    }
        
    
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
    	 for (PageId pageId : pageIdToPages.keySet()) {
    		 
    		 Page page = pageIdToPages.get(pageId);
    		
    		 if (page.isDirty() == tid) {
    		
    			 flushPage(pageId);
    		
    		 }
    		
    	 }
    		
   }	
    	
    private boolean isDirty(PageId pageId) {
    	
    	return pageIdToPages.get(pageId).isDirty() != null;
    	
    }
    
 

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */

    // For Lab 4 it is crucial that evicted pages do not belong to uncommitted transactions if they are dirty
    private synchronized  void evictPage() throws DbException {
    	
    	Iterator<PageId> pageIdIterator = pageIdToPages.keySet().iterator();
    	
    	PageId pageId = null;
    	
    	while (pageIdIterator.hasNext()) {
    	
    		pageId = pageIdIterator.next();
    	
    		if (!isDirty(pageId)) {
    	
    			break;
    	
    		}
    	
    	}
    	
    	if (pageId == null || isDirty(pageId)) {
    	
    		throw new DbException("All pages in BufferPool are dirty--none can be evicted.");
    	
    	}
    	
    	try {
    	
    		flushPage(pageId);
    	
    	} catch (IOException e) {
    	
    		e.printStackTrace();
    	
    		throw new DbException("IOException while flushing page during eviction.");
    	
    	}
    	
    	pageIdToPages.remove(pageId);
    	
    	currentPages.decrementAndGet();
    	
    }
    	
}
    
    	
    	