package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
       return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int tableId = pid.getTableId();
        int pageNo = pid.getPageNumber();

        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file ,"r");
            if((long) (pageNo + 1) *BufferPool.getPageSize()>f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNo));
            }

            byte[] bytes = new byte[BufferPool.getPageSize()];

            // don't seek one by one, because of may be out of memory.
            f.seek((long) pageNo *BufferPool.getPageSize());

            int read = f.read(bytes, 0, BufferPool.getPageSize());

            if(read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNo));
            }

            HeapPageId id = new HeapPageId(tableId, pageNo);

            return new HeapPage(id, bytes);
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            try{
                assert f != null;
                f.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pageNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) Math.floor((this.file.length() * 1.0) / (BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(this, tid);
    }


    private static final class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;
        private final TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;
        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNum) throws TransactionAbortedException, DbException {
            if(pageNum>=0 && pageNum < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNum);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d does not contain page %d.", pageNum, heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it==null){
                return false;
            }

            // maybe have more than one page in the DbFile.
            if(!it.hasNext()) {
                if(whichPage < (heapFile.numPages() - 1)) {
                    whichPage ++;
                    it = getPageTuples(whichPage);
                    return it.hasNext();
                }else{
                    return false;
                }
            }else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }else{
                return it.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }
}

