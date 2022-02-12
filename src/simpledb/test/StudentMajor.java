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

         //selected id and name from student
//         String myQuery = "SELECT sname, sid FROM STUDENT ";
         //SORTING TEST
         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname asc, sid desc";
         //Creating a query Plan
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);

         //Scanning result set
         Scan resultScanner = myPlan.open();

         System.out.println("student_name\tsid");
         while (resultScanner.next()) {
//            String studentName = resultScanner.getString("sname");
//            String departmentName = resultScanner.getString("dname");
//            System.out.println(studentName + "\t\t\t\t" + departmentName);
            String studentName = resultScanner.getString("sname");
            Integer sid = resultScanner.getInt("sid");
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

