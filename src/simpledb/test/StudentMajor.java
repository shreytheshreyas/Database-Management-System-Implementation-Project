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
//         String myQuery = "SELECT Sname, Dname FROM STUDENT, DEPT";
//                 + " WHERE MajorId = DId";

         //ORDER BY TEST
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sid ";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname, sid desc";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid asc";
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid desc";
//         String myQuery = "SELECT sname, majorid, gradyear FROM STUDENT ORDER BY sname asc, majorid asc, gradyear desc";


         //INNER EQUI JOIN TEST - NEED to add INNER JOIN keyword to the list of keywords
//         String myQuery = "SELECT deptid, did, title, dname FROM dept JOIN course ON deptid = did WHERE did > 20";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM student JOIN enroll ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON studentid = sid";
         
//         String myQuery = "SELECT distinct majorid FROM student WHERE gradyear = 2022 or sid = 3";
//         String myQuery = "SELECT sid FROM student, dept, course WHERE deptid = did";
         String myQuery = "select distinct sname from student, enroll where sid = studentid order by sname desc";
//         select distinct sname from student, enroll where sid = studentid order by sname desc
         
         //Scanning result set

         //Creating a query Plan
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();
//         System.out.println(myPlan);
         System.out.println("deptid\tdid\ttitle\tdname");
         while (resultScanner.next()) {
//            Integer deptid = resultScanner.getInt("deptid");
//            Integer did = resultScanner.getInt("did");
//            String title = resultScanner.getString("title");
//            String dname = resultScanner.getString("dname");

//            Integer sid = resultScanner.getInt("eid");
//            Integer studentid = resultScanner.getInt("studentid");
            String name = resultScanner.getString("sname");
//            String grade = resultScanner.getString("grade");
//            System.out.println(deptid+ "\t\t\t\t"+ did + "\t\t\t\t" + title + "\t\t\t\t" + dname);
            System.out.println(name);
//            System.out.println(sid+ "\t\t\t\t"+ studentid + "\t\t\t\t" + name + "\t\t\t\t" + grade);
//            System.out.println(studentName+ "\t\t\t\t" + sid);

         }

         resultScanner.close();
         myTransaction.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}

