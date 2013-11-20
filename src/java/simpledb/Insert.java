package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId m_tid;
    DbIterator m_child;
    int m_tableId;
    TupleDesc m_td;
    boolean fn_calledBefore = false;
    
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	m_tid = t;
    	m_child = child;
    	m_tableId = tableid;
    	Type[] typeAr = { Type.INT_TYPE };
    	String[] fieldAr = { "Count" };
    	m_td = new TupleDesc(typeAr, fieldAr);	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.open();
    	super.open();
    	fn_calledBefore = false;
    }

    public void close() {
        // some code goes here
    	m_child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (fn_calledBefore)
    		return null;
    	fn_calledBefore = true;
    	int count = 0;
    	while(m_child.hasNext()){
    		Tuple t = m_child.next();
    		try {
    			Database.getBufferPool().insertTuple(m_tid, m_tableId, t);
    			count++;
    		} catch ( IOException e ){ }
    	}
    	Tuple t = new Tuple(m_td);
    	t.setField(0, new IntField(count));
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] { this.m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if (this.m_child != children[0])
    	{
    	    this.m_child = children[0];
    	}
    }
}
