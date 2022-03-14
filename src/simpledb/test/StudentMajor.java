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

         
//         String myQuery = "select sname, prof from student, enroll, section where sid = studentid and sectionid = sectid";
         String myQuery = "select sname, prof, sectionid from student, enroll, section where sid = studentid AND sectionid = sectid and sectionid = 43";
//         String myQuery = "select sid from student, enroll, section where sid = studentid and yearoffered = gradyear and sid = 3";
//         String myQuery = "select sname, prof from student, enroll, section where sectionid = sectid AND sid = studentid";
//         String myQuery = "select sid from student, enroll, section where sid = studentid and yearoffered = gradyear";
//         String myQuery = "select student.sname from student where student.sid = 5";
//         String myQuery = "select sname from student where sid = 5";
         
         
         //INNER EQUI JOIN TEST - NEED to add INNER JOIN keyword to the list of keywords
//         String myQuery = "select distinct did from dept";
//         String myQuery = "SELECT distinct deptid, did, title, dname FROM dept JOIN course ON did = deptid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM student JOIN enroll ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON studentid = sid";
         
//        
         
         //Scanning result set

         //Creating a query Plan
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();
         QueryPlanOutput.getFinalOutput();
         System.out.println(" ");
         System.out.println("sname"+ "\t\t\t\t");
         while (resultScanner.next()) {
//            Integer deptid = resultScanner.getInt("deptid");
//            Integer did = resultScanner.getInt("did");
//            String title = resultScanner.getString("title");
//        	 String prof = resultScanner.getString("prof");
//        	 Integer majorid = resultScanner.getInt("majorid");
//            String dname = resultScanner.getString("dname");
        	 
        	 String sname = resultScanner.getString("sname");
             String prof = resultScanner.getString("prof");
             Integer sectionid = resultScanner.getInt("sectionid");
        	 System.out.println(sname + "\t\t\t\t" + prof + "\t\t\t\t" + sectionid);
        	 
        	 
//            Integer sid = resultScanner.getInt("sid");
//            System.out.println(sid + "\t\t\t\t");
            
//            Integer studentid = resultScanner.getInt("studentid");
//            String name = resultScanner.getString("sname");
//            String grade = resultScanner.getString("grade");
        	 
//            System.out.println(deptid+ "\t\t\t\t"+ did + "\t\t\t\t" + title + "\t\t\t\t" + dname);
//            System.out.println(deptid);
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

