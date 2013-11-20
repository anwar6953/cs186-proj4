package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> TDItemArr;
    
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDItemArr.iterator();
        // return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDItemArr = new ArrayList<TDItem>();
        
        // If the entire fieldAr is null, make it empty strings.
        boolean one_param_cons = true;
        for (int i = 0; i < fieldAr.length; i++)
            if (fieldAr[i] != null)
                one_param_cons = false;
        if (one_param_cons == true)
            for (int i = 0; i < fieldAr.length; i++)
                fieldAr[i] = "";
        // If the entire fieldAr is null, make it empty strings.        
        
        int len = typeAr.length;
        for (int i = 0; i < len; i++){
            TDItemArr.add(new TDItem(typeAr[i],fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        if (TDItemArr != null)
            return TDItemArr.size();
        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= TDItemArr.size()){
            throw new NoSuchElementException();
        }
        return TDItemArr.get(i).fieldName;
        // return null;
    }
    

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= TDItemArr.size())
            throw new NoSuchElementException("i: " + i + ", size: " + TDItemArr.size());
        return TDItemArr.get(i).fieldType;
        // return null;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null)
            throw new NoSuchElementException(name + " is not a valid field name");
            
        for (int i = 0; i < TDItemArr.size(); i++){
            if (TDItemArr.get(i).fieldName != null && TDItemArr.get(i).fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException(name + " is not a valid field name");
        // return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < TDItemArr.size(); i++){
            size += TDItemArr.get(i).fieldType.getLen();
        }
        return size;
        // return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        
        int len = td1.TDItemArr.size();
        Type[] typeAr = new Type[len];
        String[] fieldAr = new String[len];
        
        for (int i = 0; i < len; i++){
            typeAr[i] = td1.TDItemArr.get(i).fieldType;
            fieldAr[i] = td1.TDItemArr.get(i).fieldName;
        }
        
        TupleDesc td3 = new TupleDesc(typeAr,fieldAr);
        td3.TDItemArr.addAll(td2.TDItemArr);
        return td3;
        // return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc){
            int len = TDItemArr.size();
            if (numFields() != ((TupleDesc) o).numFields())
                return false;
            for (int i = 0; i < len; i++){
                if (getFieldType(i) != ((TupleDesc) o).getFieldType(i))
                    return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    String output = "";
	for(int i = 0; i < TDItemArr.size(); i++){
		output += TDItemArr.get(i).fieldType.toString() + "(" + TDItemArr.get(i).fieldName.toString() + "),";
	}
        return output;
    }
}
