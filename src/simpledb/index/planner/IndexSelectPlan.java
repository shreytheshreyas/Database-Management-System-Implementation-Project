package simpledb.index.planner;

import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexSelectScan;

/** The Plan class corresponding to the <i>indexselect</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class IndexSelectPlan implements Plan {
   private Plan p;
   private IndexInfo ii;
   private Constant val;
   private Predicate pred;
   private String planType1;
   
   /**
    * Creates a new indexselect node in the query tree
    * for the specified index and selection constant.
    * @param p the input table
    * @param ii information about the index
    * @param val the selection constant
    * @param tx the calling transaction 
    */
   public IndexSelectPlan(Plan p, IndexInfo ii, Constant val, Predicate pred) {
      this.p = p;
      this.ii = ii;
      this.val = val;
      this.pred = pred;
   }
   
   public String getPlanType() {
	   return planType1;
   }
   
   /** 
    * Creates a new indexselect scan for this query
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      // throws an exception if p is not a tableplan.
      TableScan ts = (TableScan) p.open();
      String scanString1 = String.valueOf(ts);
      planType1 = (scanString1.split("@")[0]).split("\\.")[2];
      Index idx = ii.open();
      return new IndexSelectScan(ts, idx, val, pred);
   }
   
   /**
    * Estimates the number of block accesses to compute the 
    * index selection, which is the same as the 
    * index traversal cost plus the number of matching data records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return ii.blocksAccessed() + recordsOutput();
   }
   
   /**
    * Estimates the number of output records in the index selection,
    * which is the same as the number of search key values
    * for the index.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return ii.recordsOutput();
   }
   
   /** 
    * Returns the distinct values as defined by the index.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return ii.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the data table.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return p.schema(); 
   }
}
