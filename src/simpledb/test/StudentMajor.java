package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

import java.sql.*;

public class StudentMajor {
   public static void main(String[] args) {
      try {
         // created database
         SimpleDB myDatabase = new SimpleDB("studentdb");

         // created transaction and planner
         Transaction myTransaction = myDatabase.newTx();
         Planner myPlanner = myDatabase.planner();

         String myQuery = "select sid, sname, majorid, dname, title, cid, sectionid, grade from student, dept, course, enroll where sid = studentid and did = deptid and majorid = did order by sid, sname, majorid, dname, title, cid, sectionid, grade";
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();
         QueryPlanOutput.getFinalOutput();
         System.out.println(" ");

         // System.out.println(myPlan);
         System.out.println("count");
         while (resultScanner.next()) {
        	 Integer sid = resultScanner.getInt("sid");
        	 String sname = resultScanner.getString("sname");
        	 Integer majorid = resultScanner.getInt("majorid");
        	 String dname = resultScanner.getString("dname");
            String title = resultScanner.getString("title");
            Integer cid = resultScanner.getInt("cid");
            Integer sectionid = resultScanner.getInt("sectionid");
            String grade = resultScanner.getString("grade");
            System.out.println(sid + "|" + sname + "|" + majorid+ "|" + dname+ "|" + title+ "|" + cid+ "|" + sectionid+ "|" + grade);

            
         }

         System.out.println("Query was a success");
         resultScanner.close();
         myTransaction.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
