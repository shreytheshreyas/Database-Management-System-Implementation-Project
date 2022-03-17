package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class HashPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private Predicate joinpred;
   private int noOfPartitions;
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx the calling transaction
    */
   public HashPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, boolean isDistinct, Predicate joinpred) {
      this.fldname1 = fldname1;
      this.p1 = p1;
      
      this.fldname2 = fldname2;
      this.p2 = p2;
      
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      
      this.joinpred = joinpred;
      
      this.noOfPartitions = tx.availableBuffs() - 1;
   }
   
   
   public String getPlanType() {
	   return "";
   }
   
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      String joinString1 = String.valueOf(s1);
      String joinString2 = String.valueOf(s2);
      QueryPlanOutput.putJoinPlan("HashJoinPlan");
      QueryPlanOutput.putFinalJoinPred(joinpred.toString());
      QueryPlanOutput.putScanPlan((joinString1.split("@")[0]).split("\\.")[2] + " on " + p1.schema().getTableName(), 
    		  (joinString2.split("@")[0]).split("\\.")[2] + " on " +  p2.schema().getTableName());
      return new HashScan(s1, fldname1, s2, fldname2, noOfPartitions, p1.schema().fields());
   }
   
   /**
    * Return the number of block acceses required to
    * mergejoin the sorted tables.
    * Since a mergejoin can be preformed with a single
    * pass through each table, the method returns
    * the sum of the block accesses of the 
    * materialized sorted tables.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return (p1.blocksAccessed() + p2.blocksAccessed()) * 3;
   }
   
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() * p2.recordsOutput();
   }
   
   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}



