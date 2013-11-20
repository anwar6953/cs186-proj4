package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Integer, Integer> groups; //key: group value, value: [function, count]
    private Map<Integer, StringField> hashstr; 
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        //System.out.println("gbfieldtype: " + gbfieldtype.toString());
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groups = new HashMap<Integer, Integer>();
        hashstr = new HashMap<Integer, StringField>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int bucket = -1;
        if (gbfield != NO_GROUPING) {
            if (gbfieldtype == Type.STRING_TYPE) {
                bucket = ((StringField)tup.getField(gbfield)).hashCode();
                hashstr.put(((StringField)tup.getField(gbfield)).hashCode(), ((StringField)tup.getField(gbfield)));

            }
            else {
                bucket = ((IntField)tup.getField(gbfield)).getValue();
            }
        }
        int aggval = 0;
        int gkey = 0;
        if (gbfieldtype == Type.STRING_TYPE) {
            gkey =  ((StringField)tup.getField(gbfield)).hashCode();
        }
        else {
            gkey = ((IntField)tup.getField(gbfield)).getValue();
        }

        if (groups.containsKey(gkey)) {
            groups.put(bucket, groups.get(bucket) + 1);
        }
        else {
            groups.put(bucket, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for proj2");
        List<Tuple> tuparr = new ArrayList<Tuple>();
        Type[] typearr = new Type[]{gbfieldtype, Type.INT_TYPE};
        TupleDesc fortup = new TupleDesc(typearr, new String[]{null, null});
        Object[] keys = groups.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {
            
            int key = (Integer) keys[i];
            
            Tuple tup = new Tuple(fortup);
            if (gbfieldtype == Type.STRING_TYPE) {
                tup.setField(0, hashstr.get(key));
            }
            else {
                tup.setField(0, new IntField(key));
            }
            //tup.setField(0, new IntField(key));
            tup.setField(1, new IntField(groups.get(key)));
            tuparr.add(tup);            

        }
        List<Tuple> immutable = Collections.unmodifiableList(tuparr);
        TupleIterator tupiter = new TupleIterator(fortup, immutable);
        if (gbfield == NO_GROUPING) {
            List<Tuple> tuparr1 = new ArrayList<Tuple>();
            TupleDesc fortup1 = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
            Tuple tup1 = new Tuple(fortup1);
            tup1.setField(0, new IntField(groups.get(-1)));
            tuparr1.add(tup1);
            List<Tuple> immutable1 = Collections.unmodifiableList(tuparr1);
            TupleIterator tupiter1 = new TupleIterator(fortup1, immutable1);
            
            return (DbIterator) tupiter1;
        }
        return (DbIterator) tupiter;
    }

}


