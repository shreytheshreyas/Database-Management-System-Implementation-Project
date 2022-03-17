package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;
import simpledb.query.QueryPlanOutput;
import simpledb.materialize.*;
import simpledb.materialize.DistinctPlan;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   HashMap<TablePlanner, String> tableNames = new HashMap<TablePlanner, String>();
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    * 
    * select sname, prof from student, enroll, section where sid = studentid AND sectionid = sectid
    */
   public Plan createPlan(QueryData data, Transaction tx) {
	  QueryPlanOutput.putAllPred(data.pred());
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm, data.isDistinct()); //here
         tableNames.put(tp, tblname);
         QueryPlanOutput.putTables(tblname);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();
      //System.out.println("select (" + data.pred().toString() + ")");
      
      // Step 3:  Repeatedly add a plan to the join order
      //currentplan = (enrollxsection)xstudent
      Integer count = tableplanners.size();
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  { // no applicable join
            currentplan = getLowestProductPlan(currentplan);
         }
      }
      
      //String joinString = String.valueOf(currentplan);
      //System.out.println((joinString.split("@")[0]).split("\\.")[2]);

      
      String planTypeString = currentplan.getPlanType();
      System.out.println(planTypeString);
      
      // Step 5: Add a distinct plan if isDistinct is true
      if (data.isDistinct()) {
    	 LinkedHashMap<String, String> test = new LinkedHashMap<>();
    	 for (String field : data.fields()) {
    		 test.put(field, "asc");
    	}
         currentplan = new DistinctPlan(tx, currentplan, test); //here
      }
      
      //
      // Step 4: Checking if the query has order by
      if(data.hasOrderFields()) {
         currentplan = new SortPlan(tx, currentplan, data.orderFields(), data.isDistinct());
      }

      currentplan = new ProjectPlan(currentplan, data.fields());

      // Step 5.  Project on the field names and return

//      return new ProjectPlan(currentplan, data.fields());
      //NEW STEP - checking if the query needs to have a group by plan
      if(data.hasGroupByFields() || data.hasAggFields()) {
         currentplan = new GroupByPlan(tx, currentplan, data.getGroupByFields(), data.getAggFunctions());
      }

//      currentplan = new ProjectPlan(currentplan, data.fields());
      return currentplan;
   }

   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);
         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
            besttp = tp;
            bestplan = plan;
         }
      }
      
      if (bestplan != null)
         tableplanners.remove(besttp);
      
      return bestplan;
   }
   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current, current.schema());
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
