package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Integer, int[]> groups; //key: group value, value: [function, count]
    private Map<Integer, StringField> hashstr; 


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groups = new HashMap<Integer, int[]>();
        hashstr = new HashMap<Integer, StringField>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tuple
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //System.out.println("fix what: " + what.toString().equals("sum"));
        
        int bucket = -1;
        int aggval = 0;
        int gkey = -1;

        if (gbfield != NO_GROUPING) {
            if (gbfieldtype == Type.STRING_TYPE) {
                bucket = ((StringField)tup.getField(gbfield)).hashCode();
                hashstr.put(((StringField)tup.getField(gbfield)).hashCode(), ((StringField)tup.getField(gbfield)));
            }
            else {
                bucket = ((IntField)tup.getField(gbfield)).getValue();
            }
        

            if (gbfieldtype == Type.STRING_TYPE) {
                gkey =  ((StringField)tup.getField(gbfield)).hashCode();
            }
            else {
                gkey = ((IntField)tup.getField(gbfield)).getValue();
            }
        }
        if (groups.containsKey(gkey)){
            if (what.toString().equals("count")) {
                aggval = 0;
            }
            else if (what.toString().equals("sum")) {
                aggval = groups.get(bucket)[0] + ((IntField)tup.getField(afield)).getValue();
            }
            else if (what.toString().equals("avg")){
                aggval = groups.get(bucket)[0] + ((IntField)tup.getField(afield)).getValue();
            }
            else if (what.toString().equals("min")) {
                aggval = Math.min(groups.get(bucket)[0], ((IntField)tup.getField(afield)).getValue());
            }
            else if (what.toString().equals("max")) {
                aggval = Math.max(groups.get(bucket)[0], ((IntField)tup.getField(afield)).getValue());

            }
            groups.put(bucket, new int[] {aggval, groups.get(bucket)[1] + 1});
        }
        else {
            groups.put(bucket, new int[] {((IntField)tup.getField(afield)).getValue(), 1});
        } 
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

        List<Tuple> tuparr = new ArrayList<Tuple>();
        Type[] typearr;
        if (gbfield == NO_GROUPING) {
            typearr = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
        }
        else {
            typearr = new Type[]{gbfieldtype, Type.INT_TYPE};
        }
        TupleDesc fortup = new TupleDesc(typearr, new String[]{null, null});
        Object[] keys = groups.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {

            int key = (Integer) keys[i];
            int[] next = groups.get(key);
            
            Tuple tup = new Tuple(fortup);
            if (gbfieldtype == Type.STRING_TYPE) {
                tup.setField(0, hashstr.get(key));
            }
            else {
                tup.setField(0, new IntField(key));
            }
            
            if (what.toString().equals("avg")) {
                tup.setField(1, new IntField(next[0]/next[1]));
            }
            else if (what.toString().equals("count")) {
                tup.setField(1, new IntField(next[1]));
            }
            else{
                tup.setField(1, new IntField(next[0]));
            }

            tuparr.add(tup);         

        }
        List<Tuple> immutable = Collections.unmodifiableList(tuparr);
        TupleIterator tupiter = new TupleIterator(fortup, immutable);
        if (gbfield == NO_GROUPING) {
            List<Tuple> tuparr1 = new ArrayList<Tuple>();
            TupleDesc fortup1 = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
            Tuple tup1 = new Tuple(fortup1);
            tup1.setField(0, new IntField(groups.get(-1)[0]/groups.get(-1)[1]));
            tuparr1.add(tup1);
            List<Tuple> immutable1 = Collections.unmodifiableList(tuparr1);
            TupleIterator tupiter1 = new TupleIterator(fortup1, immutable1);
            //tupiter1.open();
            //System.out.println("tupiter: " + tupiter1.next());

            return (DbIterator) tupiter1;
        }
        return (DbIterator) tupiter;


        //throw new
        //UnsupportedOperationException("please implement me for proj2");
        //return agg.iterator();
        /*
        System.out.println("entrySet: " + groups.entrySet().toArray().toString());
        DbIterator it = (DbIterator) groups.entrySet().iterator();
        if (it.hasNext()) {
                    System.out.println("entrySet: " + (Map.Entry)it.next());

        }

        return it;
        */
        /*
        DbIterator it = new DbIterator() {      
            
            Iterator iter = groups.keySet().iterator();
            boolean state = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                iter = groups.keySet().iterator();
                state = true;
            }
            
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // TODO Auto-generated method stub
                if (!state) {
                    return false;
                }
                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException,
                    NoSuchElementException {
                if (!state) {
                    return null;
                }
                if (iter.hasNext()) {
                    System.out.println("test");
                    int key = (Integer) iter.next();
                    int[] next = groups.get(key);
                    Type[] typearr = new Type[]{gbfieldtype, gbfieldtype};
                    TupleDesc fortup = new TupleDesc(typearr, new String[]{null, null});
                    Tuple tup = new Tuple(fortup);
                    tup.setField(0, new IntField(key));
                    tup.setField(1, new IntField(next[0]));
                    //System.out.println("tuple1: " + tup.toString());
                    return tup;
                    
                }
                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iter = groups.keySet().iterator();
            }
            
            public TupleDesc getTupleDesc(){
                return null;
            }

            @Override
            public void close() {
                state = false;
            }
        };
        return it;
        */
    }

}