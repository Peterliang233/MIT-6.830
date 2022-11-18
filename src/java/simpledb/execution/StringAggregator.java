package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;

    private final Type gbFieldType;

    private final int afield;

    private Aggregator.Op op;

    private final AggHandler aggHandler;

    private abstract class AggHandler {
        HashMap<Field, Integer> res;

        abstract void handler(Field field, StringField stringField);

        public AggHandler() {
            res = new HashMap<>();
        }

        HashMap<Field, Integer> getRes() {
            return this.res;
        }
    }

    class CountHandler extends AggHandler {
        @Override
        void handler(Field field, StringField stringField) {
            if(res.containsKey(field)) {
                res.put(field, res.get(field) + 1);
            }else{
                res.put(field, 1);
            }
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if (what == Op.COUNT) {
            this.aggHandler = new CountHandler();
        }else{
            throw new NoSuchElementException("No such aggregate operation.");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        StringField stringField = (StringField) tup.getField(this.afield);
        Field field = this.gbField == NO_GROUPING ? null : tup.getField(this.gbField);
        this.aggHandler.handler(field, stringField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        Type[] types;
        String[] fields;

        HashMap<Field, Integer> res = this.aggHandler.getRes();

        List<Tuple> tuples = new ArrayList<>();

        TupleDesc tupleDesc;

        if (this.gbField == NO_GROUPING) {
            // no grouping
            types = new Type[]{Type.INT_TYPE};
            fields = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, fields);
            IntField intField = new IntField(res.get(null));
            Tuple tp = new Tuple(tupleDesc);
            tp.setField(0, intField);
            tuples.add(tp);
        }else{
            // have grouping
            types = new Type[]{this.gbFieldType, Type.INT_TYPE};
            fields = new String[]{"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, fields);
            for(Field field:res.keySet()) {
                Tuple tp = new Tuple(tupleDesc);
                if(this.gbFieldType == Type.INT_TYPE) {
                    IntField intField = (IntField) field;
                    tp.setField(0, intField);
                }else{
                    StringField stringField = (StringField) field;
                    tp.setField(0, stringField);
                }

                IntField intField = new IntField(res.get(field));
                tp.setField(1, intField);
                tuples.add(tp);
            }
        }

        return new TupleIterator(tupleDesc, tuples);
    }

}
