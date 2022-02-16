package simpledb.test;


import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;


import java.sql.*;

public class StudentMajor {
   public static void main(String[] args) {
      try{
         //created database
         SimpleDB myDatabase = new SimpleDB("studentdb");

         //created transaction and planner
         Transaction myTransaction = myDatabase.newTx();
         Planner myPlanner = myDatabase.planner();

         //ORIGINAL SQL query statement
//         String myQuery = "SELECT Sname, Dname FROM STUDENT, DEPT"
//                 + " WHERE MajorId = DId";

         //ORDER BY TEST
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sid ";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname, sid desc";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid asc";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid desc";
//         String myQuery = "SELECT sname, majorid, gradyear FROM STUDENT ORDER BY sname asc, majorid asc, gradyear desc";


         //INNER EQUI JOIN TEST - NEED to add INNER JOIN keyword to the list of keywords
         String myQuery = "SELECT dept.did, title, dname FROM DEPT JOIN COURSE ON dept.did = course.did";
         //Scanning result set

         //Creating a query Plan
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();

         System.out.println("student_name\tsid");
         while (resultScanner.next()) {
            String studentName = resultScanner.getString("sname");
//            String departmentName = resultScanner.getString("dname");
//            System.out.println(studentName + "\t\t\t\t" + departmentName);
//            String studentName = resultScanner.getString("sname");
//            Integer majorid = resultScanner.getInt("majorid");
            Integer sid = resultScanner.getInt("sid");
//            Integer gradyear = resultScanner.getInt("gradyear");
//            System.out.println(studentName+ "\t\t\t\t" + majorid + "\t\t\t\t" + gradyear);
            System.out.println(studentName+ "\t\t\t\t" + sid);
         }

         resultScanner.close();
         myTransaction.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}

