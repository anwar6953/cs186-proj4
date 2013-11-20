package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    DbIterator m_child;
    int m_afield;
    int m_gfield;
    Aggregator.Op m_aop;
    TupleDesc m_td;
    IntegerAggregator intagg;
    StringAggregator stragg;

    DbIterator striter;
    DbIterator intiter;
    boolean state = false;

    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    // some code goes here



        m_child = child;
        m_afield = afield;
        m_gfield = gfield;
        m_aop = aop;
        
        Type[] typeAr;
        String[] fieldAr;

        if (gfield == Aggregator.NO_GROUPING){
            typeAr = new Type[]{ Type.INT_TYPE };
            fieldAr = new String[]{ null };
        } else {
            String aggrStr = nameOfAggregatorOp(aop) + m_child.getTupleDesc().getFieldName(afield);
            String groupStr = m_child.getTupleDesc().getFieldName(gfield);
            Type aggrType = m_child.getTupleDesc().getFieldType(afield);
            Type groupType = m_child.getTupleDesc().getFieldType(gfield);
            typeAr = new Type[]{groupType, aggrType};
            fieldAr = new String[]{ groupStr , aggrStr };
        }
        m_td = new TupleDesc(typeAr, fieldAr);
        
        if (m_child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            stragg = new StringAggregator(gfield, m_child.getTupleDesc().getFieldType(gfield), afield, aop);
        }
        else {
            if (gfield == -1) {
                intagg = new IntegerAggregator(gfield, null, afield, aop);
            }
            else {
                intagg = new IntegerAggregator(gfield, m_child.getTupleDesc().getFieldType(gfield), afield, aop);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    // some code goes here
        return m_gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    // some code goes here
        if (m_gfield == Aggregator.NO_GROUPING)
            return null;
        return m_child.getTupleDesc().getFieldName(m_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    // some code goes here
        return m_afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    // some code goes here
        return m_child.getTupleDesc().getFieldName(m_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    // some code goes here
        return m_aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
        TransactionAbortedException {
    // some code goes here
        m_child.open();
        if (m_child.getTupleDesc().getFieldType(m_afield) == Type.STRING_TYPE) {
            while (m_child.hasNext()){
                //System.out.println("child.next: " + m_child.next());
                stragg.mergeTupleIntoGroup(m_child.next());
            }
            m_child.close();
            striter = stragg.iterator(); 
            striter.open();  
            super.open();
        }
        else {
            while (m_child.hasNext()){
                intagg.mergeTupleIntoGroup(m_child.next());
            } 
            m_child.close();
            intiter = intagg.iterator();
            intiter.open();
            super.open();
        }
        state = true;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    //TODO
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    // some code goes here
        if (m_child.getTupleDesc().getFieldType(m_afield) == Type.STRING_TYPE) {
            if(striter.hasNext()){
                return striter.next();
            }
        }
        else {
            if(intiter.hasNext()){
                return intiter.next();
            }
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    // some code goes here
        if (m_child.getTupleDesc().getFieldType(m_afield) == Type.STRING_TYPE) {
            striter.rewind();
        }
        else {
            intiter.rewind();
        } 
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    // some code goes here
        return m_td;
    }

    public void close() {
    // some code goes here
        if (m_child.getTupleDesc().getFieldType(m_afield) == Type.STRING_TYPE) {
            striter.close();
            super.close();
        }
        else {
            intiter.close();   
            super.close();      
        }    
    }

    @Override
    public DbIterator[] getChildren() {
    // some code goes here
        return new DbIterator[] { this.m_child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    // some code goes here
        if (this.m_child!=children[0])
        {
            this.m_child = children[0];
        }
    }
    
}



