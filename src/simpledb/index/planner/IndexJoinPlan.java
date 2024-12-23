package simpledb.index.planner;

import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.index.Index;
import simpledb.index.query.IndexJoinScan;

/** The Plan class corresponding to the <i>indexjoin</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class IndexJoinPlan implements Plan {
   private Plan p1, p2;
   private IndexInfo ii;
   private String joinfield;
   private Predicate joinpred;
   private Schema sch = new Schema();
   private String planType1, planType2;
   
   /**
    * Implements the join operator,
    * using the specified LHS and RHS plans.
    * @param p1 the left-hand plan
    * @param p2 the right-hand plan
    * @param ii information about the right-hand index
    * @param joinfield the left-hand field used for joining
    */
   public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinfield, Predicate joinpred) {
      this.p1 = p1;
      this.p2 = p2;
      this.ii = ii;
      this.joinfield = joinfield;
      this.joinpred = joinpred;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   public String getPlanType() {
	   return planType1 + "|"+ planType2;
   }
   
   /**
    * Opens an indexjoin scan for this query
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p1.open();
      String scanString1 = String.valueOf(s);
      planType1 = (scanString1.split("@")[0]).split("\\.")[2];
      // throws an exception if p2 is not a tableplan
      TableScan ts = (TableScan) p2.open();
      String scanString2 = String.valueOf(ts);
      planType2 = (scanString2.split("@")[0]).split("\\.")[2];
      Index idx = ii.open();
      
      String joinString1 = String.valueOf(s);
      String joinString2 = String.valueOf(ts);
      System.out.println(joinpred.toString());
      QueryPlanOutput.putJoinPlan("IndexJoinPlan");
      QueryPlanOutput.putFinalJoinPred(joinpred.toString());
      QueryPlanOutput.putScanPlan((joinString1.split("@")[0]).split("\\.")[2] + " on " + p1.schema().getTableName(), 
    		  (joinString2.split("@")[0]).split("\\.")[2] + " on " +  p2.schema().getTableName());
      return new IndexJoinScan(s, idx, joinfield, ts);
   }
   
   /**
    * Estimates the number of block accesses to compute the join.
    * The formula is:
    * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
    *       + R(indexjoin(p1,p2,idx) </pre>
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() 
         + (p1.recordsOutput() * ii.blocksAccessed())
         + recordsOutput();
   }
   
   /**
    * Estimates the number of output records in the join.
    * The formula is:
    * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() * ii.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct values for the 
    * specified field.  
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the index join.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}
