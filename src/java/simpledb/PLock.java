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
//    		System.out.println(name + name.length());
	    	int numTabs = Integer.parseInt(name.substring(7, name.length()));
	    	
	    	for (int i=0; i<numTabs; i++)
	    		tabs += "\t";
	    	
	    	for (int i=0; i<numTabs; i++)
	    		tabs += "         ";
    	}
    	System.out.println(tabs + s);
    }
    
    public static void mustHold(boolean b, String s){
    	if (!b)
    		for (int i=0; i<8; i++)
    			System.out.println("\n" + s);
//    		System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n" + s);
    }

    //returns when lock is obtained. If not obtainable, Sleeps or blocks until then.
    public static void acquireLock(TransactionId tid, PageId pid, Permissions perm){
    	while (!gotLock(tid, pid, perm)){
    		//sleep
    		synchronized (pid) {
    			log("sleeping.");
        		try { pid.wait(); } catch (InterruptedException e1) { e1.printStackTrace(); }	
			}
    		log("Awoke.");
//    		try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
    	}
    	log("Locked.");
    	return;
    }
    
    //Returns true if lock obtained, or false if must block/sleep.
    public static boolean gotLock(TransactionId tid, PageId pid, Permissions perm){
    	synchronized (pid) {
    		
    		if (pid.pageNumber() == 0)
        	System.out.println("tid: " + tid.getId() + ", pid: " + pid.pageNumber() + ", Perm: " + perm);
		ConcurrentHashMap<PLock, Boolean> lockSet = m_lockHash.get(pid);
		
    	if (lockSet == null){
    		lockSet = new ConcurrentHashMap<PLock, Boolean>();
    		m_lockHash.put(pid, lockSet);
    		PLock.lockIt(tid, pid, perm);
    		return true;
    	} 
    	
    	//lockSet is not null now.
		if (lockSet.containsKey(new PLock(tid, pid, perm))){
			return true;  // This Transaction already has a lock
		}
		
		if (lockSet.containsKey(new PLock(tid, pid, Permissions.READ_ONLY)) || lockSet.containsKey(new PLock(tid, pid, Permissions.READ_WRITE))){
			System.out.println("containsKey");
			if (lockSet.size() <= 1){
				releaseLock(tid, pid);
				acquireLock(tid, pid, perm);
				return true;
			}
			return false;    				
		}
		
		if (lockSet.size() != 0){
			if (perm == Permissions.READ_WRITE){
				System.out.println("test" + lockSet.size());
				return false;
			}
			else if (lockSet.size() == 1){
	    		if (lockSet.keySet().iterator().next().getPerm() == Permissions.READ_WRITE){
	    			return false;
	    		} else { PLock.lockIt(tid, pid, perm); return true; }
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
    static public void releaseLock(TransactionId tid, PageId pid){
    	ConcurrentHashMap<PLock, Boolean> lockSet = m_tidToLocks.get(tid);
    	Iterator<PLock> it = lockSet.keySet().iterator();
    	
    	while (it.hasNext()){
    		PLock l = it.next();
    		PageId pgid = l.pid;
    		if (pid == null || pgid.equals(pid)){
    			log("Released.");
	    		ConcurrentHashMap<PLock, Boolean> lockSet2 = m_lockHash.get(pgid);
	    		Boolean result1 = lockSet2.remove(new PLock(tid, pid, Permissions.READ_ONLY));
	    		Boolean result2 = lockSet2.remove(new PLock(tid, pid, Permissions.READ_WRITE));
	    		Boolean result3 = lockSet.remove(new PLock(tid, pid, Permissions.READ_ONLY));
	    		Boolean result4 = lockSet.remove(new PLock(tid, pid, Permissions.READ_WRITE));
	    		
	    		//Ensure that two keys were removed:
	    		int x  = (result1!=null?1:0) + (result2!=null?1:0);
	    		x += (result3!=null?1:0) + (result4!=null?1:0);
	    		mustHold(x==2, "2 entires were not removed in releaseLock");
	    		
	        	synchronized (pgid) {
	        		pid.notifyAll();
	    		}
    		}
    	}
    	
    	if (pid == null)
    		mustHold(lockSet.size() == 1, "all locks for a tid should've been released.");
    	
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
		private Permissions perm;
		private PageId pid;
		
		public testClass(TransactionId tid, Permissions perm, PageId pid){
			this.tid = tid;
			this.perm = perm;
			this.pid = pid;
		}
		
		public void run(){
			Thread t = Thread.currentThread();
			PLock.acquireLock(tid, pid, perm);
			try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
			PLock.releaseLock(tid, pid);
		}
	}
	
	public static void main(String [ ] args) throws Exception
	{
		TransactionId tid1 = new TransactionId();
		TransactionId tid2 = new TransactionId();
		TransactionId tid3 = new TransactionId();
		
		HeapPageId pid1 = new HeapPageId(0, 0);
		HeapPageId pid2 = new HeapPageId(0, 0);
		
		Permissions perm_r = Permissions.READ_ONLY;
		Permissions perm_w = Permissions.READ_WRITE;

		Thread t1 = new Thread(new testClass(tid1, perm_r, pid1));
		Thread t2 = new Thread(new testClass(tid2, perm_r, pid1));
		Thread t3 = new Thread(new testClass(tid3, perm_w, pid1));
		t1.start();
		t2.start();
		t3.start();
		t1.join();
		t2.join();
		t3.join();
		
	}
}


