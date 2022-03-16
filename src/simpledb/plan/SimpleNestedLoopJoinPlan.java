package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.QueryPlanOutput;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class SimpleNestedLoopJoinPlan implements Plan {
    private Transaction tx;
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
    public SimpleNestedLoopJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, Predicate joinpred) {
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.p1 = p1;
        this.p2 = p2;
        this.tx = tx;
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
        Scan s1 = p1.open();
        String scanString1 = String.valueOf(s1);
        planType1 = (scanString1.split("@")[0]).split("\\.")[2];
        TableScan s2 = (TableScan) p2.open();
        String scanString2 = String.valueOf(s2);
        planType2 = (scanString2.split("@")[0]).split("\\.")[2];
        
        String joinString1 = String.valueOf(s1);
        String joinString2 = String.valueOf(s2);
        QueryPlanOutput.putJoinPlan("SimpleNestedLoopJoinPlan");
        QueryPlanOutput.putFinalJoinPred(joinpred.toString());
        QueryPlanOutput.putScanPlan((joinString1.split("@")[0]).split("\\.")[2] + " on " + p1.schema().getTableName(), 
      		  (joinString2.split("@")[0]).split("\\.")[2] + " on " +  p2.schema().getTableName());
        return new SimpleNestedLoopJoinScan(tx, s1, s2, fldname1, fldname2); //need to change later
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
        return p1.blocksAccessed() + p2.blocksAccessed(); //for query optimiser
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

