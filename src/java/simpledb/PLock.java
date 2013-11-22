package simpledb;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class PLock {

	private TransactionId tid;
	private Permissions perm;
	private PageId pid;

    private static ConcurrentHashMap<PageId, ConcurrentHashMap<PLock, Boolean>> m_lockHash = new ConcurrentHashMap<PageId, ConcurrentHashMap<PLock, Boolean>>();
    private static ConcurrentHashMap<TransactionId, ConcurrentHashMap<PLock, Boolean>> m_tidToLocks = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PLock, Boolean>>();
    
    public static void reset(){
    	m_lockHash = new ConcurrentHashMap<PageId, ConcurrentHashMap<PLock, Boolean>>();
    	m_tidToLocks = new ConcurrentHashMap<TransactionId, ConcurrentHashMap<PLock, Boolean>>();
    }
    
    public static void log(String s){
    	String tabs = "";
    	String name = Thread.currentThread().getName();
    	if (!name.equals("main")){ 
	    	int numTabs = Integer.parseInt(name.substring(7, name.length()));
	    	
	    	for (int i=0; i<numTabs; i++)
	    		tabs += "            ";
    	}
    	System.out.println(tabs + s + "[" + Thread.currentThread().getId() + "]");
    }
    
    public static void mustHold(boolean b, String s){
    	if (!b)
    		for (int i=0; i<1; i++)
    			System.out.println("\n[" + Thread.currentThread().getId() + "]" + s);
//    		System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n" + s);
    }

    //returns when lock is obtained. If not obtainable, Sleeps or blocks until then.
    public static void acquireLock(TransactionId tid, PageId pid, Permissions perm){
    	while (!gotLock(tid, pid, perm)){
			log("sleeping.");
			ConcurrentHashMap<PLock, Boolean> lockSet = m_lockHash.get(pid);
    		synchronized (lockSet) { try { lockSet.wait(); } catch (InterruptedException e1) { e1.printStackTrace(); } }
    		log("Awoke.");
    	}
    	log("Locked_" + (perm==Permissions.READ_WRITE?"X":"S") + "(" + pid.pageNumber() + ")");
    	return;
    }
    
    //Returns true if lock obtained, or false if must block/sleep.
    public static boolean gotLock(TransactionId tid, PageId pid, Permissions perm){
    	ConcurrentHashMap<PLock, Boolean> lockSet = m_lockHash.get(pid);
    	if (lockSet == null){
    		lockSet = new ConcurrentHashMap<PLock, Boolean>();
    		m_lockHash.put(pid, lockSet);
    	}
    	
    	synchronized (lockSet) {
	    		
	    	//lockSet is not null:
			if (lockSet.containsKey(new PLock(tid, pid, perm))){
				return true;  // This Transaction already has a lock
			}
			
			//If lockSet contains a Write Lock from this trans on this page, 
			//we need not do anything (because gotLock is attempting to acquire a read lock).
			if (lockSet.containsKey(new PLock(tid, pid, Permissions.READ_WRITE))){
				return true;
			}
			//IF lockSet contains a READ lock, that means we must upgrade it, if possible.
			if (lockSet.containsKey(new PLock(tid, pid, Permissions.READ_ONLY))){
				if (lockSet.size() <= 1){
					releaseLock(tid, pid);
					acquireLock(tid, pid, perm);
					return true;
				}
				return false;    				
			}
			
			//tid doesn't have any existing lock on the page:
			//if lockSet.size == 0, skip this, and place the lock.
			if (lockSet.size() != 0){
				//if WRITE lock is requested, it can ONLY be granted if lockSet.size == 0
				if (perm == Permissions.READ_WRITE){
					return false;
				}
				
				//otherwise, we're trying to get a READ lock. We can only get this, if none
				//of the current locks are write locks, we check this by checking if there's one lock
				//and its a write lock (can't have write lock if lockSet.size() > 1).
				else if (lockSet.size() == 1){
		    		if (lockSet.keySet().iterator().next().getPerm() == Permissions.READ_WRITE){
		    			return false;
		    		}
				}
			}
			PLock.lockIt(tid, pid, perm);
			return true;
    	
    	}
    }
    
    //Actually acquires the lock
    static private void lockIt(TransactionId tid, PageId pid, Permissions perm){
    	PLock l = new PLock(tid, pid, perm);
    	m_lockHash.get(pid).put(l, new Boolean(true));

    	ConcurrentHashMap<PLock, Boolean> lockSet2 = m_tidToLocks.get(tid);
    	if (lockSet2 == null){
    		lockSet2 = new ConcurrentHashMap<PLock, Boolean>();
    		m_tidToLocks.put(tid,lockSet2);
    	}
    	m_tidToLocks.get(tid).put(l, new Boolean(true));
    }
    
    //Actually releases the lock associated with Transaction tid AND on page w/ PageId pid.
    //pid == null means to unlock all of tid's locks (on all pages).
    static public void releaseLock(TransactionId tid, PageId pid){
    	ConcurrentHashMap<PLock, Boolean> lockSet = m_tidToLocks.get(tid);
    	Iterator<PLock> it = lockSet.keySet().iterator();

    	while (it.hasNext()){
    		PLock l = it.next();
    		PageId pgid = l.pid;
    		if (pid == null || pgid.equals(pid)){
    			log("Released(" + pgid.pageNumber() + ")");
	    		ConcurrentHashMap<PLock, Boolean> lockSet2 = m_lockHash.get(pgid);
	    		Boolean result1 = lockSet2.remove(new PLock(tid, pgid, Permissions.READ_ONLY));
	    		Boolean result2 = lockSet2.remove(new PLock(tid, pgid, Permissions.READ_WRITE));
	    		Boolean result3 = lockSet.remove(new PLock(tid, pgid, Permissions.READ_ONLY));
	    		Boolean result4 = lockSet.remove(new PLock(tid, pgid, Permissions.READ_WRITE));
	    		
	    		//Ensure that two keys were removed:
//	    		int x  = (result1!=null?1:0) + (result2!=null?1:0);
//	    		x += (result3!=null?1:0) + (result4!=null?1:0);
//	    		mustHold(x==2, "2 entires were not removed in releaseLock" + x);
	    		
	        	synchronized (lockSet2) {
	        		lockSet2.notifyAll();
	    		}
    		}
    	}
    	
//    	if (pid == null)
//    		mustHold(lockSet.size() == 0, "all locks for a tid should've been released.");
    	
    }
    
    static public boolean holdsLock(TransactionId tid, PageId pid){
    	ConcurrentHashMap<PLock, Boolean> lockSet = m_tidToLocks.get(tid);

    	if (lockSet != null){
	    	Iterator<PLock> it = lockSet.keySet().iterator();
	    	
	    	while (it.hasNext()){
	    		if (it.next().getPid().equals(pid)){
	    			return true;
	    		}
	    	}
    	}
    	return false;
    }
    
    //release all locks associated with Transaction tid
    static public void releaseByTrans(TransactionId tid){
    	releaseLock(tid, null);
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((perm == null) ? 0 : perm.hashCode());
		result = prime * result + ((pid == null) ? 0 : pid.hashCode());
		result = prime * result + ((tid == null) ? 0 : tid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PLock other = (PLock) obj;
		if (perm == null) {
			if (other.perm != null)
				return false;
		} else if (!perm.equals(other.perm))
			return false;
		if (pid == null) {
			if (other.pid != null)
				return false;
		} else if (!pid.equals(other.pid))
			return false;
		if (tid == null) {
			if (other.tid != null)
				return false;
		} else if (!tid.equals(other.tid))
			return false;
		return true;
	}

	public PLock(TransactionId tid, PageId pid, Permissions perm){
		this.tid = tid;
		this.perm = perm;
		this.pid = pid;
	}

	public TransactionId getTid(){
		return this.tid;
	}

	public Permissions getPerm(){
		return this.perm;
	}
	
	public PageId getPid(){
		return this.pid;
	}

	private static class testClass implements Runnable {
		private TransactionId tid;
		private int id;
		
		public testClass(int pid){
			this.tid = new TransactionId();;
			this.id = pid;
		}
		
		public void run(){
			Permissions perm_r = Permissions.READ_ONLY;
			Permissions perm_w = Permissions.READ_WRITE;
			
			HeapPageId pid1 = new HeapPageId(0, 0);
			HeapPageId pid2 = new HeapPageId(0, 1);
			HeapPageId pid3 = new HeapPageId(0, 2);
			
			log("" + tid.myid);
			if (id == 0){
//				mustHold(PLock.holdsLock(tid, pid1)==false,"ERROR");
//				mustHold(PLock.holdsLock(tid, pid2)==false,"*ERROR*");
//				mustHold(PLock.holdsLock(tid, pid3)==false,"ERROR**");
				
				PLock.acquireLock(tid, pid1, perm_r);
//				mustHold(PLock.holdsLock(tid, pid1)==true,"*ERROR*");
//				mustHold(PLock.holdsLock(tid, pid2)==false,"*ERROR**");
				PLock.acquireLock(tid, pid2, perm_r);
//				mustHold(PLock.holdsLock(tid, pid2)==true,"**ERROR*");
				PLock.acquireLock(tid, pid3, perm_r);
//				mustHold(PLock.holdsLock(tid, pid1)==true,"**ERROR*");
//				mustHold(PLock.holdsLock(tid, pid2)==true,"*ERROR**");
//				mustHold(PLock.holdsLock(tid, pid3)==true,"*ERROR**");
			}
			if (id == 1){
				try { Thread.sleep(100); } catch (InterruptedException e) { e.printStackTrace(); }
//				mustHold(PLock.holdsLock(tid, pid1)==false,"ERROR***");
				PLock.acquireLock(tid, pid1, perm_w);
//				mustHold(PLock.holdsLock(tid, pid1)==true,"**ERROR*");
			}
			try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
//			PLock.releaseLock(tid, pid1);
			PLock.releaseByTrans(tid);
//			mustHold(PLock.holdsLock(tid, pid1)==false,"*ERROR**");
//			mustHold(PLock.holdsLock(tid, pid2)==false,"*ERROR**");
//			mustHold(PLock.holdsLock(tid, pid3)==false,"*ERROR**");
		}
	}
	
	public static void main(String [ ] args) throws Exception
	{
		Thread t1 = new Thread(new testClass(0));
		Thread t2 = new Thread(new testClass(1));
//		Thread t3 = new Thread(new testClass(2));
		t1.start();
		t2.start();
//		t3.start();
		t1.join();
		t2.join();
//		t3.join();
		
	}
}


