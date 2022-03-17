package simpledb.query;

import java.util.ArrayList;
import java.util.List;

public class QueryPlanOutput {
	public static String finalOutput = "";
	public static String selectPlan = "";
	public static ArrayList<String> tables = new ArrayList<String>();
	public static List<String> allPredTerms = new ArrayList<String>();
	public static ArrayList<String> selectPred = new ArrayList<String>();
	public static ArrayList<String> finalJoinPred = new ArrayList<String>();
	public static ArrayList<String> scanPlan = new ArrayList<String>();
	public static ArrayList<String> joinPlans = new ArrayList<String>();
	
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
//		for (Term term : terms) {
//			String LHS = term.getLhs().toString();
//			String RHS = term.getRhs().toString();
//			try {
//				// ATTRIBUTE EQUATE TO INTEGER
//				Integer d = Integer.parseInt(RHS);
//		        if (d.getClass().getSimpleName().equals("Integer")) {
//		        	selectPred.add(term.toString());
//		        } 
////		        else if () {
////		        	
////		        }
//		        
//		        // ATTRIBUTE EQUATES TO ANOTHER ATTRIBUTE IN THE SAME TABLE
//		    } catch (NumberFormatException nfe) {
//		        joinPred.add(term.toString());
//		    }
//		}
	}
	
	public static void putFinalJoinPred(String pred) {
		// joinPred -> sid=studentid and sectionid=sectid
		// field1 = studentid field2 = sid
		// finaljoinPred.add(sid=studentid)
//		for (String term : joinPred) {
//			String[] fields = term.split("=");
//			if ( (field1.equals(fields[0]) || field1.equals(fields[1])) && (field2.equals(fields[0]) || field2.equals(fields[1])) ) {
//				finalJoinPred.add(term);
//			}
//		}
		finalJoinPred.add(pred);
		
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
		System.out.print("select (" + finalSelectPred + ") ");
		
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
	
			System.out.print(output);
			System.out.println(" ");
		}
		// CASES WHERE THERE ARE NO JOINS (1 TABLE ONLY)
		else {
			String tableNames = String.join(", ", tables);
			output = "[" + tableNames + "]";
			
			System.out.print(output);
			System.out.println(" ");
		}
		
		finalSelectPred = "";
		finalOutput = "";
		selectPlan = "";
		tables = new ArrayList<String>();
		allPredTerms = new ArrayList<String>();
		selectPred = new ArrayList<String>();
		finalJoinPred = new ArrayList<String>();
		scanPlan = new ArrayList<String>();
		joinPlans = new ArrayList<String>();
		
		
	}
}


