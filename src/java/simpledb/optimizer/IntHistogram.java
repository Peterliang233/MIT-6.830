package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;

    private final int max;

    private final int bucketNum;

    private int[] buckets;

    private final double width;

    private int ntups;  // the number of tuples in the table.

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.bucketNum = buckets;
        this.max = max;
        this.min = min;
        this.buckets = new int[buckets];
        this.width = (max - min + 1.0) / buckets;
        this.ntups = 0;
    }


    public int getIdx(int v){

        return (int) ((v - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        if(v >= min && v <= max){
            this.buckets[getIdx(v)] ++;
            this.ntups ++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // TODO: some code goes here
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if(v <= min) return 0.0;
            if(v >= max) return 1.0;

            double sum = 0.0;
            int idx = getIdx(v);
            for(int i=0;i<idx;i++){
                sum += this.buckets[i];
            }

            sum += (v - min - idx * width) * (1.0 * this.buckets[idx]/width);

            return sum / ntups;
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }

        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        }

        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
        }

        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }

        if(op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                    estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }

        throw new UnsupportedOperationException("Operation is illegal.");
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        int sum = 0;
        for(int i=0;i<this.bucketNum;i++){
            sum += this.buckets[i];
        }

        return (double) sum / this.ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return String.format("IntHistogram min: %d, max: %d\n", min, max);
    }
}
