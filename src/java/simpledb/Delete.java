package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId m_tid;
    DbIterator m_child;
    TupleDesc m_td;
    boolean fn_calledBefore = false;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	m_tid = t;
    	m_child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (fn_calledBefore)
    		return null;
    	fn_calledBefore = true;
    	int count = 0;
    	while(m_child.hasNext()){
    		Tuple t = m_child.next();
    		Database.getBufferPool().deleteTuple(m_tid, t);
    		count++;
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
