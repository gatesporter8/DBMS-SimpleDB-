package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private TupleDesc returnTD;
    private TransactionId tid;
    private boolean processed=false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.child = child;
        this.tid = t;
        
        // we return a 1-field tuple
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        this.returnTD = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        return returnTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.close();
        child.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        if (processed)
            return null;
        
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
            	Database.getBufferPool().deleteTuple(tid, t);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            count++;
        }

        // finished scanning
        // generate a new "delete count" tuple
        Tuple tup = new Tuple(returnTD);
        tup.setField(0, new IntField(count));
        processed=true;
        return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
