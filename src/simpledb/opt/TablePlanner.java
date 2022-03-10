package simpledb.opt;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

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
      Plan p = makeIndexSelect();
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

      //queryJoinPlan = makeSortMergeJoin(current, currsch); //This is for the sort merge join plan
      queryJoinPlan = makeSortMergeJoin(current, currsch); //index join - done
      //queryJoinPlan = makeNestedLoopJoin(current, currsch); //This is for the nested loop join plan

      //if (queryJoinPlan == null)
         //queryJoinPlan = makeProductJoin(current, currsch);
      return queryJoinPlan;
   }

   /* Make three of the following classes to implement Nested Loop Join:
   * 1. NLJsimple - for each tuple in the outer relation it checks all the tuples in the inner relation
   * 2. NLJpage - for each page in table in the outer relation you scan all the pages of the inner relation, in
   *    contrast to NLJsimple where for each tuple of the outer relation you scan all the pages of the inner relation
   * 3. NLJblock*/

   //TODO: NEED TO IMPLEMENT FUNCTION DEFINITION
   private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
      boolean joinCondition = false;
      //Query Optimiser will decide which type of NLJ w

      //get predicate terms
      List<Term> predicateTerms = mypred.getTerms();

      //1. Get LHS field of the predicate
      String lhsField = predicateTerms.get(0).getLhs().asFieldName();
      System.out.println(lhsField);

      //2. Get RHS field of the predicate
      String rhsField = predicateTerms.get(0).getRhs().asFieldName();
      System.out.println(rhsField);

      //3. if both exist in their respective tables we call the SimpleNestedJoinPlan
      if(myschema.hasField(lhsField) && currsch.hasField(rhsField))
         return new SimpleNestedLoopJoinPlan(tx, current, myplan, rhsField, lhsField);
      else if(myschema.hasField(rhsField) && currsch.hasField(lhsField))
         return new SimpleNestedLoopJoinPlan(tx, current, myplan, lhsField, rhsField);

      return null;
   }

   //TODO: NEED TO IMPLEMENT FUNCTION DEFINITION
   private Plan makeSortMergeJoin(Plan current, Schema currsch) {
      boolean joinCondition = false;

      //get predicate terms
      List<Term> predicateTerms = mypred.getTerms();

      //algorithm

      //1. Get LHS field of the predicate
      String lhsField = predicateTerms.get(0).getLhs().asFieldName();
      System.out.println(lhsField);

      //2. Get RHS field of the predicate
      String rhsField = predicateTerms.get(0).getRhs().asFieldName();
      System.out.println(rhsField);

      //3. if both exist in their respective tables we call the MergeJoinPlan
      if(myschema.hasField(lhsField) && currsch.hasField(rhsField))
         return new MergeJoinPlan(tx, current, myplan, rhsField, lhsField, isDistinct); //here
      else if(myschema.hasField(rhsField) && currsch.hasField(lhsField))
         return new MergeJoinPlan(tx, current, myplan, lhsField, rhsField, isDistinct); //here

      return null;
   }

   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
//            addQueryComponent("Index Select Plan on" + fldname)
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
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
