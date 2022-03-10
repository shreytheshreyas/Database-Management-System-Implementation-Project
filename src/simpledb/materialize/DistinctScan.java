package simpledb.materialize;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
/**
 * @author sciore
 *
 */
public class DistinctScan implements Scan {
   private Scan s1;
   
   /**
    * Create a sort scan, given a list of 1 or 2 runs.
    * If there is only 1 run, then s2 will be null and
    * hasmore2 will be false.
    * @param runs the list of runs
    * @param comp the record comparator
    */
   public DistinctScan(List<TempTable> runs) {
	  this.s1 = (Scan) runs.get(0).open();

   }
   
   /**
    * Position the scan before the first record in sorted order.
    * Internally, it moves to the first record of each underlying scan.
    * The variable currentscan is set to null, indicating that there is
    * no current scan.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
	   s1.beforeFirst();
   }
   
   /**
    * Move to the next record in sorted order.
    * First, the current scan is moved to the next record.
    * Then the lowest record of the two scans is found, and that
    * scan is chosen to be the new current scan.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
	   return s1.next();
   }
   
   /**
    * Close the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
	   s1.close();
   }
   
   /**
    * Get the Constant value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return s1.getVal(fldname);
   }
   
   /**
    * Get the integer value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return s1.getInt(fldname);
   }
   
   /**
    * Get the string value of the specified field
    * of the current scan.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return s1.getString(fldname);
   }
   
   /**
    * Return true if the specified field is in the current scan.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname);
   }
}
