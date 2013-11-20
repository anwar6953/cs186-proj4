package simpledb;

import java.security.NoSuchAlgorithmException;
import java.util.*;

import javax.xml.crypto.Data;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

	TransactionId m_transId;
	int m_tblId;
	String m_tableAlias;
	
	int m_open = 0;
	DbFileIterator m_titr = null;
	
    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	m_transId = tid;
    	m_tblId = tableid;
    	m_tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	return Database.getCatalog().getTableName(m_tblId);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        // some code goes here
        return m_tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	m_tblId = tableid;
    	m_tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
//    	String tblName = Database.getCatalog().getTableName(m_tblId);
    	DbFile file = Database.getCatalog().getDbFile(m_tblId);
    	m_titr = file.iterator(m_transId);
    	m_titr.open();
    	m_open = 1;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc old = Database.getCatalog().getDbFile(m_tblId).getTupleDesc();
    	int numFields = old.numFields();
        String[] stringarr = new String[numFields];
        Type[] types = new Type[numFields];
        
        for (int i = 0; i < numFields; i++) {
            stringarr[i] = getAlias() + "." + old.getFieldName(i);
            types[i] = old.getFieldType(i);
        }
    	return new TupleDesc( types, stringarr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_open != 1)
    		return false;
        return m_titr.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	if (hasNext())
    		return m_titr.next();
    	
        throw new NoSuchElementException();
    }

    public void close() {
        // some code goes here
    	m_titr = null;
    	m_open = 0;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }
}
