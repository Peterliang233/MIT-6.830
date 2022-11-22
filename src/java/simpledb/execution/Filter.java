package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final TupleDesc td;

    private Iterator<Tuple> it;

    private final List<Tuple> tuples = new ArrayList<>();

    private final Predicate predicate;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
        this.child = child;
        this.predicate = p;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        child.open();
        while(child.hasNext()) {
            Tuple tp = child.next();
            if (this.predicate.filter(tp)) {
                tuples.add(tp);
            }
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        }else{
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child = children[0];
    }

}
