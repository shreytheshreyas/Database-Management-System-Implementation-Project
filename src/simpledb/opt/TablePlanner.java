package simpledb.opt;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import simpledb.materialize.HashPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   private boolean isDistinct;
   private ArrayList<String> queryPlanComponents = new ArrayList<String>();
   private List<Term> tempPredicateTerms = new ArrayList<Term>();
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm, boolean isDistinct) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
      this.isDistinct = isDistinct;
   }
   
   public void addQueryComponent(String component) {
	   queryPlanComponents.add(component);
   }
   
   public ArrayList<String> getQueryComponents() {
	   return queryPlanComponents;
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
	   Plan p = null;
//	   if (mypred.toString() == "") {
	   p = makeIndexSelect();
//	   }
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;

      Plan queryJoinPlan = null;

      /*The query optimiser will choose which of the following join plans is ideal
      * for a required query that is provided to the database*/
     
      Plan sortMergeJoinPlan = makeSortMergeJoin(current, currsch, joinpred);
      Plan indexJoinPlan  = makeIndexJoin(current, currsch, joinpred);
      Plan nestedLoopJoinPlan = makeNestedLoopJoin(current, currsch, joinpred);
//      Plan hashJoinPlan = makeHashJoin(current, currsch, joinpred);
      int sortMergeJoinPlanCost = -1;
      int indexJoinPlanCost = -1;
      int nestedLoopJoinPlanCost = -1;
      int hashJoinPlanCost = -1;
      int minimumCost = 0;
      LinkedHashMap<Plan, Integer> comparisonArray = new LinkedHashMap<Plan, Integer>();

      if (sortMergeJoinPlan != null) {
         sortMergeJoinPlanCost = sortMergeJoinPlan.blocksAccessed();
         comparisonArray.put(sortMergeJoinPlan, sortMergeJoinPlanCost);
      }
      	

      if (indexJoinPlan != null) {
         indexJoinPlanCost = indexJoinPlan.blocksAccessed();
	     comparisonArray.put(indexJoinPlan, indexJoinPlanCost);
	   }

      if (nestedLoopJoinPlan != null) {
         nestedLoopJoinPlanCost = nestedLoopJoinPlan.blocksAccessed();
         comparisonArray.put(nestedLoopJoinPlan, nestedLoopJoinPlanCost);
	   }
      
//      if (hashJoinPlan != null) {
//    	  hashJoinPlanCost = hashJoinPlan.blocksAccessed();
//          comparisonArray.put(hashJoinPlan, hashJoinPlanCost);
// 	   }
      
      int count = 0;
      for (Map.Entry mapElement : comparisonArray.entrySet()) {
    	  Plan plan = (Plan) mapElement.getKey();
          int cost = (Integer) mapElement.getValue();
          if (count == 0) {
        	  minimumCost = cost;
        	  queryJoinPlan = plan;
          } else if (cost < minimumCost) {
        	  minimumCost = cost;
        	  queryJoinPlan = plan;  
          }
          count++;
      }

      if (queryJoinPlan == null)
         queryJoinPlan = makeProductJoin(current, currsch);
      queryJoinPlan = nestedLoopJoinPlan;
      return queryJoinPlan;
   }

   /* Make three of the following classes to implement Nested Loop Join:
   * 1. NLJsimple - for each tuple in the outer relation it checks all the tuples in the inner relation
   * 2. NLJpage - for each page in table in the outer relation you scan all the pages of the inner relation, in
   *    contrast to NLJsimple where for each tuple of the outer relation you scan all the pages of the inner relation
   * 3. NLJblock*/

   //TODO: NEED TO IMPLEMENT FUNCTION DEFINITION
   private Plan makeNestedLoopJoin(Plan current, Schema currsch, Predicate joinpred) {
      boolean joinCondition = false;
      //Query Optimiser will decide which type of NLJ w

      //get predicate terms
      List<Term> predicateTerms = mypred.getTerms();
      for (Term term : predicateTerms) {
         String lhsField = term.getLhs().asFieldName();
         String rhsField = term.getRhs().asFieldName();
         //3. if both exist in their respective tables we call the SimpleNestedJoinPlan
         if (myschema.hasField(lhsField) && currsch.hasField(rhsField)) {
            Plan p = new SimpleNestedLoopJoinPlan(tx, current, myplan, rhsField, lhsField, joinpred);
         	p = addSelectPred(p);
	        return addJoinPred(p, currsch);
         }
         else if (myschema.hasField(rhsField) && currsch.hasField(lhsField)) {
            Plan p = new SimpleNestedLoopJoinPlan(tx, current, myplan, lhsField, rhsField, joinpred);
            p = addSelectPred(p);
	        return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   //TODO: NEED TO IMPLEMENT FUNCTION DEFINITION
   private Plan makeSortMergeJoin(Plan current, Schema currsch, Predicate joinpred) {
      boolean joinCondition = false;

      //get predicate terms
      List<Term> predicateTerms = mypred.getTerms();
      for (Term term : predicateTerms) {
         String lhsField = term.getLhs().asFieldName();
         String rhsField = term.getRhs().asFieldName();
         
         //3. if both exist in their respective tables we call the MergeJoinPlan
         // current is the CURRENT PLAN, my plan is the incoming one
         if (myschema.hasField(lhsField) && currsch.hasField(rhsField)) {
        	Plan p = new MergeJoinPlan(tx, current, myplan, rhsField, lhsField, isDistinct, joinpred); //here
	        p = addSelectPred(p);
	        return addJoinPred(p, currsch);
         }
         else if (myschema.hasField(rhsField) && currsch.hasField(lhsField)) {
        	Plan p = new MergeJoinPlan(tx, current, myplan, lhsField, rhsField, isDistinct, joinpred); //here
         	p = addSelectPred(p);
	        return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeHashJoin(Plan current, Schema currsch, Predicate joinpred) {
	      boolean joinCondition = false;

	      //get predicate terms
	      List<Term> predicateTerms = mypred.getTerms();
	      for (Term term : predicateTerms) {
	         String lhsField = term.getLhs().asFieldName();
	         String rhsField = term.getRhs().asFieldName();

	         //3. if both exist in their respective tables we call the MergeJoinPlan
	         // current is the CURRENT PLAN, my plan is the incoming one
	         if (myschema.hasField(lhsField) && currsch.hasField(rhsField)) {
	            Plan p = new HashPlan(tx, current, myplan, rhsField, lhsField, isDistinct, joinpred); //here
	            p = addSelectPred(p);
		        return addJoinPred(p, currsch);
	         }
	         else if (myschema.hasField(rhsField) && currsch.hasField(lhsField)) {
	            Plan p = new HashPlan(tx, current, myplan, lhsField, rhsField, isDistinct, joinpred); //here
	            p = addSelectPred(p);
		        return addJoinPred(p, currsch);
	         }
	      }
	      return null;
	   }

   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current, Schema currsch) {
      Plan p = addSelectPred(myplan);
	  return new MultibufferProductPlan(tx, current, p, isDistinct);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
//    	  System.out.println("[MakeIndexSelect()] " + fldname);
//    	  System.out.println("[MakeIndexSelect()] " + mypred);
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
//            addQueryComponent("Index Select Plan on" + fldname)
            return new IndexSelectPlan(myplan, ii, val, mypred);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch, Predicate joinpred) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, joinpred);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current, currsch);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
