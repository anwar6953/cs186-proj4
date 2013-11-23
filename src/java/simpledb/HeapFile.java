package simpledb;

import java.io.*;
import java.security.Permissions;
import java.util.*;

import simpledb.Catalog.Table;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	File m_f;
	TupleDesc m_td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	m_f = f;
    	m_td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return m_f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return m_f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	
    	HeapPage pg = null;
    	byte[] pageBytes = new byte[BufferPool.PAGE_SIZE];
    	
    	try {
	    	RandomAccessFile raf = new RandomAccessFile(m_f, "r");
	    	raf.seek(pid.pageNumber() * BufferPool.PAGE_SIZE);
	    	raf.read(pageBytes, 0, BufferPool.PAGE_SIZE);
	    	raf.close();
	    	pg = new HeapPage((HeapPageId) pid, pageBytes);
    	} catch (IOException e){
    		e.printStackTrace();
    	}
    	
    	return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    	try {
	    	RandomAccessFile raf = new RandomAccessFile(m_f, "rw");
	    	raf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
			raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
			raf.close();
    	} catch (IOException e){
    		e.printStackTrace();
    	}
    	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(m_f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	PageId pid = null;
    	HeapPage p = null;
    	ArrayList<Page> pgs = new ArrayList<Page>();
    	//figure out the pid of all the pages, and iterate over them.
    	int numPages = numPages();
    	for (int i=0; i<numPages; i++){
    		pid = new HeapPageId(this.getId(), i);
    		p = (HeapPage) Database.getBufferPool().getPage(tid, pid, simpledb.Permissions.READ_WRITE);
    		if (p.getNumEmptySlots() > 0){
    			p.insertTuple(t);
    	    	pgs.add(p);
    	    	return pgs;
    		} else {
    			//release the lock on the page, if no space is found to insert a tuple.
    			//however, this is potentially hazardous, because tid might have an older lock on it.
    			//Database.getBufferPool().releasePage(tid, pid);
    		}
    	}
		HeapPage newPg = new HeapPage(new HeapPageId(getId(), numPages), 
		HeapPage.createEmptyPageData());
		newPg.insertTuple(t);
		
		RandomAccessFile f = new RandomAccessFile(m_f, "rw");
		f.seek(numPages * BufferPool.PAGE_SIZE);
		f.write(newPg.getPageData(), 0, BufferPool.PAGE_SIZE);
		f.close();
    	
		pgs.add(newPg);
        return pgs;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	PageId pgId = t.getRecordId().getPageId();
    	HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pgId, simpledb.Permissions.READ_WRITE);
        pg.deleteTuple(t);
        return pg;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid) {    	
//         some code goes here
    	
    	DbFileIterator dbfi = new DbFileIterator() {
        	int pgno = 0;
        	HeapPage currPg = null;
        	String name = Database.getCatalog().getTableName(getId());
        	int tblid = Database.getCatalog().getTableId(name);
        	int open = 0;        	
        	
        	//Todo: pass along the tid from the outer method.
        	//DONE. by making tid param final.
        	TransactionId m_tid = tid;
        	
        	Iterator<Tuple> titr = null;
        	
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				close();
				open();
			}
			
			public void nextPage() throws DbException, TransactionAbortedException {

				if (pgno >= numPages())
					return;
				
				try {
					currPg = (HeapPage) Database.getBufferPool().getPage(m_tid, new HeapPageId(tblid, pgno), simpledb.Permissions.READ_ONLY);
					titr = currPg.iterator();
					if (titr == null)
						throw new DbException("blah blah blah");
					pgno++;
					return;
				} catch (DbException e) {
					e.printStackTrace();
				}
				
				throw new DbException("page was not found or no more iterators..?");
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				open = 1;
				pgno = 0;
				nextPage();
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException,
					NoSuchElementException {
				if (open != 1)
					throw new NoSuchElementException();
				if (!titr.hasNext())
					nextPage();
				return titr.next();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (open !=1)
					return false;
				if (titr == null) //TODO: remove
					throw new DbException(Integer.toString(pgno) + Integer.toString(open) + Integer.toString(numPages()) + m_f.getPath());
				if (!titr.hasNext())
					nextPage();
				if (titr.hasNext())
					return true;
				return false;
			}
			
			@Override
			public void close() {
				open = 0;
				titr = null;
			}
		};
        return dbfi;
    }

}

