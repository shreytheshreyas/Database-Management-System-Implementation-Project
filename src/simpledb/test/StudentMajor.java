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

//         String myQuery = "SELECT did, dname FROM DEPT WHERE dname < did";
//         String myQuery = "SELECT did, dname FROM DEPT WHERE did <= 20";
//         String myQuery = "SELECT sid FROM STUDENT";
         String myQuery = "SELECT eid FROM enroll WHERE studentId = 6";
         //TEST FOR COMPARING
//         String myQuery = "SELECT eid, did FROM ENROLL, DEPT WHERE eid < did";
         //Creating a query Plan
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);

         //Scanning result set
         Scan resultScanner = myPlan.open();

//         System.out.println("Name\tMajor");
//         System.out.println("did\tdname");
//         System.out.println("did \t departmentName");
         System.out.println("eid");
         while (resultScanner.next()) {
//            int enrollsId = resultScanner.getInt("eid");
//            int departmentId = resultScanner.getInt("did");
//            String departmentName = resultScanner.getString("dname");
            String enrollId = resultScanner.getString("eid");
//            System.out.println(departmentId + "\t" + departmentName);
//            System.out.println(enrollsId + "\t" + departmentId);
            System.out.println(enrollId);
         }
         resultScanner.close();
         myTransaction.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}

