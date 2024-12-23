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
public class MergeJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private String planType1, planType2;
   private Predicate joinpred;
   
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
   public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, boolean isDistinct, Predicate joinpred) {
      this.fldname1 = fldname1;
//      List<String> sortlist1 = Arrays.asList(fldname1);
      LinkedHashMap<String, String> sortlist1 = new LinkedHashMap<>();
      sortlist1.put(fldname1, "asc");
      this.p1 = new SortPlan(tx, p1, sortlist1, isDistinct); //here
      
      this.fldname2 = fldname2;
      LinkedHashMap<String, String> sortlist2 = new LinkedHashMap<>();
      sortlist2.put(fldname2, "asc");
      this.p2 = new SortPlan(tx, p2, sortlist2, isDistinct); //here
      
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      
      this.joinpred = joinpred;
   }
   
   
   public String getPlanType() {
	   return planType1 + "|"+ planType2;
   }
   
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
//	   System.out.println("Scan: " + p1.schema().getTableName());
//	   System.out.println("Sort Scan: " + p2.schema().getTableName());
      Scan s1 = p1.open();
//      System.out.println(s1);
      String scanString1 = String.valueOf(s1);
      planType1 = (scanString1.split("@")[0]).split("\\.")[2];
//      System.out.println(planType1);
      SortScan s2 = (SortScan) p2.open();
      String joinString1 = String.valueOf(s1);
      String joinString2 = String.valueOf(s2);
      QueryPlanOutput.putJoinPlan("MergeJoinPlan");
      QueryPlanOutput.putFinalJoinPred(joinpred.toString());
      QueryPlanOutput.putScanPlan((joinString1.split("@")[0]).split("\\.")[2] + " on " + p1.schema().getTableName(), 
    		  (joinString2.split("@")[0]).split("\\.")[2] + " on " +  p2.schema().getTableName());
      return new MergeJoinScan(s1, s2, fldname1, fldname2);
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
      return p1.blocksAccessed() + p2.blocksAccessed();
   }
   
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
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

