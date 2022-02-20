package simpledb.plan;

import simpledb.query.*;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class SimpleNestedLoopJoinScan implements Scan {
    private Transaction tx;
    private Scan s1;
    private TableScan s2;
    private String fldname1, fldname2;
    private Constant joinval = null;

    /**
     * Create a mergejoin scan for the two underlying sorted scans.
     * @param s1 the LHS sorted scan
     * @param s2 the RHS sorted scan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     */
    public SimpleNestedLoopJoinScan(Transaction tx, Scan s1, TableScan s2, String fldname1, String fldname2) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.tx = tx;
        System.out.println(tx.blockSize());
        System.out.println(tx.availableBuffs());
        this.beforeFirst();
    }

    /**
     * Close the scan by closing the two underlying scans.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s1.close();
        s2.close();
    }

    /**
     * Position the scan before the first record,
     * by positioning each underlying scan before
     * their first records.
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    /**
     * Move to the next record.  This is where the action is.
     * <P>
     * If the next RHS record has the same join value,
     * then move to it.
     * Otherwise, if the next LHS record has the same join value,
     * then reposition the RHS scan back to the first record
     * having that join value.
     * Otherwise, repeatedly move the scan having the smallest
     * value until a common join value is found.
     * When one of the scans runs out of records, return false.
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        //Algorithm
        //1. Read the block of size (bufferSize - 2) pages of outer relation -- outer for loop
        //2. Read one page of the inner relation into one buffer page -- nested for loop
        //3. for every matching record between the records in the outer relation block and
        //the records of the inner relation page, you place the record in an output buffer page -- conditional and output

//        while (true) {
//            while (s2.next()) {
//                if (s1.getVal(fldname1).equals(s2.getVal(fldname2))) {
//                    return true;
//                }
//            }
//            s2.beforeFirst();
//            if (!s1.next())
//                return false;
//        }

        s2.beforeFirst();
        while(s1.next()) {
            Constant record1 = s1.getVal(fldname1);
            while (s2.next()) {
                Constant record2 = s2.getVal(fldname2);
                if (record1.equals(record2)) {
                    return true;
                }
            }
// select * from student join enroll on sid = studentid;
// select * from enroll join student on studentid = sid;
        }

        return false;
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    /**
     * Return the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        if (s1.hasField(fldname))
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    /**
     * Return true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}

