package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>groupby</i> operator.
 * @author Edward Sciore
 */
public class GroupByPlan implements Plan {
   private Plan p;
   private List<String> groupfields;
   private List<AggregationFn> aggfns;
   private Schema sch = new Schema();
   private String planType1;
   
   /**
    * Create a groupby plan for the underlying query.
    * The grouping is determined by the specified
    * collection of group fields,
    * and the aggregation is computed by the
    * specified collection of aggregation functions.
    * @param p a plan for the underlying query
    * @param groupfields the group fields
    * @param aggfns the aggregation functions
    * @param tx the calling transaction
    */
   public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns) {
      //Converting groupfields List from List to LinkedHashMap so as to successfully execute sortPlan
	   LinkedHashMap<String, String> groupFieldsLhm = new LinkedHashMap<String, String>();
	  if (groupfields.size() > 0) {
		  groupFieldsLhm = convertListToLhm(groupfields);
	  }
      this.p = new SortPlan(tx, p, groupFieldsLhm, false);
      this.groupfields = groupfields;
      this.aggfns = aggfns;
      for (String fldname : groupfields)
         sch.add(fldname, p.schema());
      for (AggregationFn fn : aggfns)
         sch.addIntField(fn.fieldName());

   }
   
   public String getPlanType() {
	   return planType1;
   }


   private LinkedHashMap<String, String> convertListToLhm(List<String> groupfields) {
      LinkedHashMap<String, String> groupFieldsLinkedHashMap = new LinkedHashMap<>();
      for(String field : groupfields) {
         groupFieldsLinkedHashMap.put(field, "asc");
      }

      return groupFieldsLinkedHashMap;
   }

   /**
    * This method opens a sort plan for the specified plan.
    * The sort plan ensures that the underlying records
    * will be appropriately grouped.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      String scanString1 = String.valueOf(s);
      planType1 = (scanString1.split("@")[0]).split("\\.")[2];
      QueryPlanOutput.putAggTerms(aggfns);
      QueryPlanOutput.putGroupByTerms(groupfields);
      return new GroupByScan(s, groupfields, aggfns);
   }
   
   /**
    * Return the number of blocks required to
    * compute the aggregation,
    * which is one pass through the sorted table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Return the number of groups.  Assuming equal distribution,
    * this is the product of the distinct values
    * for each grouping field.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int numgroups = 1;
      for (String fldname : groupfields)
         numgroups *= p.distinctValues(fldname);
      return numgroups;
   }
   
   /**
    * Return the number of distinct values for the
    * specified field.  If the field is a grouping field,
    * then the number of distinct values is the same
    * as in the underlying query.
    * If the field is an aggregate field, then we
    * assume that all values are distinct.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p.schema().hasField(fldname))
         return p.distinctValues(fldname);
      else
         return recordsOutput();
   }
   
   /**
    * Returns the schema of the output table.
    * The schema consists of the group fields,
    * plus one field for each aggregation function.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}
