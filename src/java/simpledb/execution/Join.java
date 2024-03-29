package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final JoinPredicate p;

    private final TupleDesc td1;

    private final TupleDesc td2;

    private  OpIterator child1;

    private  OpIterator child2;

    private final int filed1;

    private final int filed2;

    private Iterator<Tuple> it;

    private final List<Tuple> tuples = new ArrayList<>();


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // TODO: some code goes here
        this.p = p;
        this.child1 = child1;
        this.td1 = child1.getTupleDesc();
        this.child2 = child2;
        this.td2 = child2.getTupleDesc();
        this.filed1 = p.getField1();
        this.filed2 = p.getField2();
    }

    public JoinPredicate getJoinPredicate() {
        // TODO: some code goes here
        return this.p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField1Name() {
        // TODO: some code goes here
        return this.td1.getFieldName(this.filed1);
    }

    /**
     * @return the field name of join field2. Should be quantified by
     *         alias or table name.
     */
    public String getJoinField2Name() {
        // TODO: some code goes here
        return this.td2.getFieldName(this.filed2);
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *         implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return TupleDesc.merge(this.td1, this.td2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        child1.open();
        child2.open();
        while(child1.hasNext()) {
            Tuple t1 = child1.next();
            while(child2.hasNext()) {
                Tuple t2 = child2.next();
                if (this.p.filter(t1,t2)) {
                    // Merge the tupleDesc of two Tuple.
                    TupleDesc td = this.getTupleDesc();
                    Tuple tuple = new Tuple(td);
                    tuple.setRecordId(t1.getRecordId());

                    // set t1 fields to tuple
                    for (int i=0;i<td1.numFields();i++){
                        tuple.setField(i, t1.getField(i));
                    }
                    // set t2 fields to tuple
                    for(int i=0;i<td2.numFields();i++){
                        tuple.setField(i+td1.numFields(), t2.getField(i));
                    }

                    this.tuples.add(tuple);
                }
            }
            // note: the child2 iterator is to the end,so we should rewind the iterator.
            child2.rewind();
        }
        it = tuples.iterator();
        super.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        it = tuples.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if(it != null && it.hasNext()) {
            return it.next();
        }

        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child1,this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
