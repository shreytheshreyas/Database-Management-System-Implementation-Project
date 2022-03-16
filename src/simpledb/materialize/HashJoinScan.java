package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinScan implements Scan {
    private Scan s1;
    private Scan s2;
    private String fldname1, fldname2;
    private int numOfPartitions;
    private int partitionIndex1;
    private int partitionIndex2;
    private Map<Constant, ArrayList<Map<String, Constant>>> partitionHashTable;
    private ArrayList<Map<String, Constant>> partitionListDetails;
    private Map<String, Constant> recordDetails;
    Plan firstTablePlanner;

    /**
     * Create a mergejoin scan for the two underlying sorted scans.
     * 
     * @param s1       the LHS sorted scan
     * @param s2       the RHS sorted scan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     */
    public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, int numOfPartitions,
            Plan firstTablePlanner) {
        this.s1 = s1;
        this.s2 = s2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.numOfPartitions = numOfPartitions - 1;
        this.partitionIndex1 = 0;
        this.partitionIndex2 = 0;
        this.firstTablePlanner = firstTablePlanner;
        beforeFirst();
    }

    /**
     * Close the scan by closing the two underlying scans.
     * 
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
     * 
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        // need to point to the first page of both partitions
        partitionIndex1++;
        nextPartition();
    }

    /**
     * Move to the next record. This is where the action is.
     * <P>
     * If the next RHS record has the same join value,
     * then move to it.
     * Otherwise, if the next LHS record has the same join value,
     * then reposition the RHS scan back to the first record
     * having that join value.
     * Otherwise, repeatedly move the scan having the smallest
     * value until a common join value is found.
     * When one of the scans runs out of records, return false.
     * 
     * @see simpledb.query.Scan#next()
     */

    private Integer h(Integer key) {
        return key % this.numOfPartitions;
    }

    private Integer h_prime(Integer key) {
        return key % this.numOfPartitions;
    }

    public boolean next() {
        // partition
        Integer bucketOne = h(s1.getVal(fldname1).hashCode());
        Integer bucketTwo = h(s2.getVal(fldname1).hashCode());

        // probe
        if (h_prime(bucketOne) == h_prime(bucketTwo)) {
            return true;
        }

        return false;

        // //Algorithm for probing
        // if(partitionListDetails != null && partitionIndex2 <
        // partitionListDetails.size()) {
        //
        // }
        //
        // //nextPartition()
        // return true;
    }

    public boolean nextPartition() {

        partitionHashTable = new HashMap<>();
        s1.beforeFirst();
        s2.beforeFirst();
        if (partitionIndex1 < numOfPartitions) {
            // Algorithm for partitioning
            while (s1.next()) {
                int s1RecordHashValue = s1.getVal(fldname1).hashCode();

                if (s1RecordHashValue % numOfPartitions == partitionIndex1) {
                    HashMap<String, Constant> s1RecordInfo = new HashMap<>();
                    for (String field : firstTablePlanner.schema().fields()) {
                        s1RecordInfo.put(field, s1.getVal(field));
                    }

                    // Need to test out this implementation
                    if (!partitionHashTable.containsKey(s1.getVal(fldname1))) {
                        ArrayList<Map<String, Constant>> partitionHashTableRecord = new ArrayList<>();
                        partitionHashTableRecord.add(s1RecordInfo);
                        partitionHashTable.put(s1.getVal(fldname1), partitionHashTableRecord);
                    }
                }
            }
            return true;
        }

        return false;
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * 
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
     * 
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
     * 
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
     * 
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}
