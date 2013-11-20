package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	int m_sizeOfBucket;
	int m_buckets;
	int m_min;
	int m_max;
	int m_numTuples;
	
	int[] m_counter;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	m_buckets = buckets;
    	m_min = min;
    	m_max = max;
    	m_numTuples = 0;

    	int range = max - min + 1;
    	int sizeOfBucket = (int) Math.ceil((float) range/buckets);
    	m_sizeOfBucket = sizeOfBucket;
    	m_counter = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int whichBucket = (v-m_min)/m_sizeOfBucket;
//    	System.out.println("whichBucket:" + whichBucket);
//    	System.out.println("numBuckets:" + m_counter.length);
    	if (whichBucket >= m_counter.length)
    		whichBucket = m_counter.length - 1;
    	assert v >= m_min;
    	assert v <= m_max;
    	m_counter[whichBucket]++;
    	m_numTuples++;
//    	System.out.println("v: " + v + ", bucket: " + whichBucket);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
//    public double estimateSelectivity(int op, int v) {
    	//Edge case, when v is smaller than or larger than all our currently-held values:
    	if (v < m_min || v > m_max){
    		if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE)
	            return 0;
	        if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ){
	        	if (v < m_min) return 1;
	        	else return 0;
	        }
	        if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ){
	        	if (v < m_min) return 0;
	        	else return 1;
	        }
	        if (op == Predicate.Op.NOT_EQUALS)
	            return 1;
    	}
    	
    	
    	
    	int whichBucket = (v-m_min)/m_sizeOfBucket;
//    	System.out.println("v: " + v + ". min: " + m_min + ". bucketSize: " + m_sizeOfBucket + ". whichBucket: " + whichBucket);
    	int h_b = m_counter[whichBucket];
    	int w_b = m_sizeOfBucket;
    	
        if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE)
            return ((float) h_b/w_b)/m_numTuples;
        if (op == Predicate.Op.NOT_EQUALS)
            return 1 - ((float) h_b/w_b)/m_numTuples;
        
        float selectivity = 0.0f;
        int b_right = m_min + (whichBucket+1)*w_b;
        int b_left = m_min + (whichBucket)*w_b - 1;  //unsure about this.
        float b_f = (float)h_b/m_numTuples;

        if (op == Predicate.Op.LESS_THAN){
        	for (int i=0; i<whichBucket; i++)
        		selectivity += (float)m_counter[i]/m_numTuples;
            return selectivity;
        }
        if (op == Predicate.Op.LESS_THAN_OR_EQ){
        	float b_part = (float)(v-b_left)/w_b;
            selectivity += b_f * b_part;
        	for (int i=0; i<whichBucket; i++)
        		selectivity += (float)m_counter[i]/m_numTuples;
            return selectivity;
        }
        if (op == Predicate.Op.GREATER_THAN){
        	for (int i=whichBucket+1; i<m_buckets; i++)
            	selectivity += (float)m_counter[i]/m_numTuples;
        	return selectivity;
        }
        if (op == Predicate.Op.GREATER_THAN_OR_EQ){
            float b_part = (float)(b_right - v)/w_b;
            selectivity += b_f * b_part;
            for (int i=whichBucket+1; i<m_buckets; i++)
            	selectivity += (float)m_counter[i]/m_numTuples;
            return selectivity;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String s = "";

    	for (int i=0; i<m_buckets; i++){
    		int val = m_counter[i];
    		if (val < 10)
    			s = s + " ";
    		s = s + val + " ";
    	}

    	s = s + "\n";
    	
    	for (int i=0; i<m_buckets; i++){
    		int val = m_min + i*m_sizeOfBucket;
    		if (i < 10)
    			s = s + " ";
    		s = s + val + " ";
    	}
    	
        return s;
    }
    
//	public static void main(String[] args){
//		IntHistogram intH = new IntHistogram(6, 1, 6);
//		intH.addValue(1);
//		intH.addValue(2);
//		intH.addValue(3);
//		intH.addValue(4);
//		intH.addValue(5);
//		intH.addValue(6);
//		System.out.println(intH.toString());
//	}
}
