package simpledb.query;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.AggregationFn;

public class QueryPlanOutput {
	public static String finalOutput = "";
	public static String selectPlan = "";
	public static ArrayList<String> tables = new ArrayList<String>();
	public static List<String> allPredTerms = new ArrayList<String>();
	public static ArrayList<String> selectPred = new ArrayList<String>();
	public static ArrayList<String> finalJoinPred = new ArrayList<String>();
	public static ArrayList<String> scanPlan = new ArrayList<String>();
	public static ArrayList<String> joinPlans = new ArrayList<String>();
	
	public static String aggFns = "";
	public static String groupBy = "";
	public static Boolean isDistinct = false;
	
//	public static void putSelectPlan(String selectPlanInput) {
//		selectPlan = selectPlanInput;
//	}
	
	public static void putTables (String tableName) {
		tables.add(tableName);
	}
	
	public static void putScanPlan(String scanPlanInputLeft, String scanPlanInputRight) {
		scanPlan.add(scanPlanInputLeft);
		scanPlan.add(scanPlanInputRight);
	}
	
	public static void putJoinPlan(String scanJoinInput) {
		joinPlans.add(scanJoinInput);
	}
	
	public static void putAllPred(Predicate allPredInput) {
		List<Term> temp = allPredInput.getTerms();
		for (Term term : temp) {
			allPredTerms.add(term.toString());
		}
	}
	
	public static void putFinalJoinPred(String pred) {
		finalJoinPred.add(pred);
	}
	
	public static void putAggTerms(List<AggregationFn> aggfns) {
		List<String> temp = new ArrayList<String>();
		for (AggregationFn fn : aggfns)
			temp.add(fn.fieldName());
		aggFns = String.join(", ", temp);
//		System.out.println(aggFns);
	}
	
	public static void putGroupByTerms(List<String> groupfields) {
		List<String> temp = new ArrayList<String>();
		if (groupfields.size() > 0) {
			for (String fldname : groupfields)
				temp.add(fldname);
			groupBy = String.join(", ", temp);
			groupBy = " (Group By: " + groupBy + ")";
		}
	}
	
	public static void putIsDistinct(Boolean distinct) {
		isDistinct = distinct;
	}
	
	//select (a>5) [(scan R) hash join (index scan on S)](c=d)
	public static void getFinalOutput() {
		
		// SELECT PREDICATE
		for (String pred : finalJoinPred) {
			if (allPredTerms.contains(pred)) {
				allPredTerms.remove(pred);
			}
		}
		String finalSelectPred = String.join(", ", allPredTerms);
		if (isDistinct) {
			System.out.print("select distinct (" + finalSelectPred + aggFns + ") ");
		} else {
			System.out.print("select (" + finalSelectPred + aggFns + ") ");
		}
		
		
		// JOIN PLAN
		int count = 0;
		String output = "";
		String firstScan = "";
		String secondScan = "";
		String joinPred;
		if (joinPlans.size() >= 1) {
			for (String joinPlan : joinPlans) {
				// INITIALISE
				firstScan = "";
				secondScan = "";
				joinPred = "";
				
				// SCAN PLAN
				if (!scanPlan.isEmpty()) {
					firstScan = scanPlan.remove(0);
					secondScan = scanPlan.remove(0);
				}
				// JOIN PREDICATE
				// TODO: ACCOUNT FOR CASES WHERE 2 TABLES JOIN ON MULTIPLE CONDITIONS
				if (!finalJoinPred.isEmpty()) {
					joinPred = finalJoinPred.remove(0);
				}
				if (count == 0) {
					output = "[(" + firstScan + ") " + joinPlan + " (" + secondScan + ")]" + "(" + joinPred + ")";
				} else {
					output = "[(" + firstScan + "(" + output + ")) " + joinPlan + " (" + secondScan + ")]" + "(" + joinPred + ")";
				}
				count++;
			}
	
			System.out.print(output + groupBy);
			System.out.println(" ");
		}
		// CASES WHERE THERE ARE NO JOINS (1 TABLE ONLY)
		else {
			String tableNames = String.join(", ", tables);
			output = "[" + tableNames + "]";
			
			System.out.print(output + groupBy);
			System.out.println(" ");
		}
		
		finalSelectPred = "";
		aggFns = "";
		finalOutput = "";
		selectPlan = "";
		tables = new ArrayList<String>();
		allPredTerms = new ArrayList<String>();
		selectPred = new ArrayList<String>();
		finalJoinPred = new ArrayList<String>();
		scanPlan = new ArrayList<String>();
		joinPlans = new ArrayList<String>();
		groupBy = "";
		
		
	}
}


