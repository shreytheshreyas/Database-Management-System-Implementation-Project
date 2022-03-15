package simpledb.query;

import java.util.ArrayList;
import java.util.List;

public class QueryPlanOutput {
	public static String finalOutput = "";
	public static String selectPlan = "";
	public static ArrayList<String> selectPred = new ArrayList<String>();
	public static ArrayList<String> joinPred = new ArrayList<String>();
	public static ArrayList<String> finalJoinPred = new ArrayList<String>();
	public static ArrayList<String> scanPlan = new ArrayList<String>();
	public static ArrayList<String> joinPlans = new ArrayList<String>();
	
//	public static void putSelectPlan(String selectPlanInput) {
//		selectPlan = selectPlanInput;
//	}
	
	public static void putScanPlan(String scanPlanInputLeft, String scanPlanInputRight) {
		scanPlan.add(scanPlanInputLeft);
		scanPlan.add(scanPlanInputRight);
	}
	
	public static void putJoinPlan(String scanJoinInput) {
		joinPlans.add(scanJoinInput);
	}
	
	public static void putGeneralSelectPred(Predicate selectPredInput) {
		List<Term> terms = selectPredInput.getTerms();
		for (Term term : terms) {
			String LHS = term.getLhs().toString();
			String RHS = term.getRhs().toString();
			try {
				Integer d = Integer.parseInt(RHS);
		        if (d.getClass().getSimpleName().equals("Integer")) {
		        	selectPred.add(term.toString());
		        }
		    } catch (NumberFormatException nfe) {
		        joinPred.add(term.toString());
		    }
		}
	}
	
	public static void putFinalJoinPred(String field1, String field2) {
		// joinPred -> sid=studentid and sectionid=sectid
		// field1 = studentid field2 = sid
		// finaljoinPred.add(sid=studentid)
		for (String term : joinPred) {
			String[] fields = term.split("=");
			if ( (field1.equals(fields[0]) || field1.equals(fields[1])) && (field2.equals(fields[0]) || field2.equals(fields[1])) ) {
				finalJoinPred.add(term);
			}
		}
		
	}
	
//	//sid=studentid and sectionid=sectid and sectionid=43
//	public static void putJoinPred(String selectPredInput) {
//		selectPred = selectPredInput;
//	}
	
	//select (a>5) [(scan R) hash join (index scan on S)](c=d)
	public static void getFinalOutput() {
//		return combination of everything
//		System.out.println(selectPlan);
//		System.out.println(selectPred);
//		System.out.println(finalJoinPred);
//		System.out.println(scanPlan);
//		System.out.println(joinPlans);

		System.out.print("select (" + selectPred + ") ");
		int count = 0;
		String output = "";
		for (String joinPlan : joinPlans) {
			String firstScan = scanPlan.remove(0);
			String secondScan = scanPlan.remove(0);
			String joinPred = finalJoinPred.remove(0);
			if (count == 0) {
				output = "[(" + firstScan + ") " + joinPlan + " (" + secondScan + ")]" + "(" + joinPred + ")";
			} else {
				output = "[(" + output + ") " + joinPlan + " (" + secondScan + ")]" + "(" + joinPred + ")";
			}
			count++;		
		}
		
		System.out.print(output);
		System.out.println(" ");
		
//		for (String joinPred : finalJoinPred) {
//			System.out.print("select (" + selectPred + ") ");
//		}
		
	}
}

