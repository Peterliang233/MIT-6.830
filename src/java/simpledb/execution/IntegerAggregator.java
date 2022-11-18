package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;

    private final int afield;

    private final Type gbType;

    private final Aggregator.Op op;

    private final AggHandler aggHandler;


    // define an abstract class for agg handler
    private abstract class AggHandler {
        HashMap<Field, Integer> res;

        abstract void handler(Field gbField,IntField intField);

        public AggHandler() {
            res = new HashMap<>();
        }

        public HashMap<Field, Integer> getRes() {
            return this.res;
        }
    }

    private class MinHandler extends AggHandler {
        @Override
        void handler(Field gbField, IntField intField) {
            int val = intField.getValue();
            if(res.containsKey(gbField)) {
                res.put(gbField, Math.min(val, res.get(gbField)));
            }else{
                res.put(gbField, val);
            }
        }
    }

    private class MaxHandler extends AggHandler {
        @Override
        void handler(Field gbField, IntField intField) {
            int val = intField.getValue();
            if(res.containsKey(gbField)) {
                res.put(gbField, Math.max(val, res.get(gbField)));
            }else{
                res.put(gbField, val);
            }
        }
    }

    private class CountHandler extends AggHandler {
        @Override
        void handler(Field gbField, IntField intField) {
            if(res.containsKey(gbField)) {
                res.put(gbField, res.get(gbField)+1);
            }else{
                res.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler {
        @Override
        void handler(Field gbField, IntField intField) {
            int val = intField.getValue();
            if(res.containsKey(gbField)) {
                res.put(gbField, res.get(gbField)+val);
            }else{
                res.put(gbField, val);
            }
        }
    }

    private class AvgHandler extends AggHandler {
        HashMap<Field, Integer> sum = new HashMap<>();
        HashMap<Field, Integer> cnt = new HashMap<>();
        @Override
        void handler(Field gbField, IntField intField) {
            int val = intField.getValue();
            if(sum.containsKey(gbField) && cnt.containsKey(gbField)) {
                sum.put(gbField, sum.get(gbField) + val);
                cnt.put(gbField, cnt.get(gbField) + 1);
            }else{
                sum.put(gbField, val);
                cnt.put(gbField, 1);
            }
            res.put(gbField, sum.get(gbField)/cnt.get(gbField));
        }
    }
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbType = gbfieldtype;
        this.op = what;
        switch (what) {
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            default:
                throw new IllegalArgumentException("No such Aggregate operation.");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        IntField intField = (IntField) tup.getField(this.afield);
        Field gbField = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);

        this.aggHandler.handler(gbField, intField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        HashMap<Field, Integer> res = this.aggHandler.getRes();

        List<Tuple> tuples = new ArrayList<>();

        Type[] types;

        String[] fields;

        TupleDesc tupleDesc;

        if (this.gbfield == NO_GROUPING) {
            // no grouping,(aggregateVal)
            types = new Type[]{Type.INT_TYPE};
            fields = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, fields);
            IntField intField = new IntField(res.get(null));
            Tuple tp = new Tuple(tupleDesc);
            tp.setField(0, intField);
            tuples.add(tp);
        }else{
            // have grouping,(groupVal,aggregateVal)
            types = new Type[]{this.gbType,Type.INT_TYPE};
            fields = new String[]{"groupVal","aggregateVal"};
            tupleDesc = new TupleDesc(types, fields);
            for(Field field: res.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                if (this.gbType == Type.INT_TYPE) {
                    IntField intField = (IntField) field;
                    tuple.setField(0, intField);
                }else{
                    StringField stringField = (StringField) field;
                    tuple.setField(0, stringField);
                }

                IntField intField = new IntField(res.get(field));
                tuple.setField(1, intField);
                tuples.add(tuple);
            }
        }

        return new TupleIterator(tupleDesc, tuples);
    }
}
