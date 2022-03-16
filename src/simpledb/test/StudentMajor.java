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
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sid "; work
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname, sid desc"; work
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid asc"; work
//         String myQuery = "SELECT sname, sid FROM STUDENT ORDER BY sname desc, sid desc"; work
//         String myQuery = "SELECT sname, majorid, gradyear FROM STUDENT ORDER BY sname asc, majorid asc, gradyear desc"; work
//         String myQuery = "select sname, prof from student, enroll, section where sid = studentid and sectionid = sectid order by sname"; work
//         String myQuery = "select sid from student, enroll, section where sid = studentid and yearoffered = gradyear and sid = 3"; not work
//         String myQuery = "select sname, prof from student, enroll, section where sectionid = sectid AND sid = studentid"; work
//         String myQuery = "select sid from student, enroll, section where sid = studentid and yearoffered = gradyear"; work
//         String myQuery = "select sname from student where sid = 5"; work

         //INNER EQUI JOIN TEST - NEED to add INNER JOIN keyword to the list of keywords
//         String myQuery = "select distinct did from dept"; work
//         String myQuery = "SELECT distinct deptid, did, title, dname FROM dept JOIN course ON did = deptid"; work
         
//         String myQuery = "SELECT distinct deptid, did, title, dname FROM dept, course where deptid = did and deptid = 30"; work
//         String myQuery = "SELECT distinct deptid, did, title, dname FROM dept, course where deptid = did and deptid > 10"; work
//         String myQuery = "SELECT deptid, did, title, dname FROM dept JOIN course ON deptid = did WHERE did > 20"; not work
//         String myQuery = "SELECT distinct deptid, did, title, dname FROM dept, course where did = deptid"; work 
         
//         String myQuery = "SELECT sid, studentid, sname, grade FROM student, enroll where sid = studentid"; work
//         String myQuery = "SELECT sid, studentid, sname, grade FROM enroll, student where sid = studentid; work
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON studentid = sid"; work


//         String myQuery = "SELECT distinct majorid FROM student WHERE gradyear = 2022 or sid = 3"; pending
         
//         String myQuery = "select distinct sname from student, enroll where sid = studentid order by sname desc"; work
//         String myQuery = "SELECT count(sid), max(sid), gradyear, min(sid), sum(sid), avg(sid) FROM student GROUP BY gradyear";
         
//         String myQuery = "SELECT sid, sname, eid, cid FROM student, course, enroll WHERE sid = studentid"; work

         //Joining Multiple Joins Table
//         String myQuery = "select sname, prof, sectionid from student, enroll, section where sid = studentid " +
//                 "AND sectionid = sectid and sectionid = 43"; work
//
         //Scanning result set

         //GROUP BY QUERY TEST
//         String myQuery = "SELECT deptid, max(cid) FROM COURSE GROUP BY deptid";

         //Creating a query Plan
         //String myQuery = "SELECT sid FROM STUDENT sid > 8";

         //Alpha Testing Queries
         String myQuery = "SELECT deptid, did, title, dname FROM dept, course where deptid = did and did > 20";
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();
         QueryPlanOutput.getFinalOutput();
         System.out.println(" ");

//         System.out.println(myPlan);
         System.out.println("sid\tsname\teid\tcid");
         while (resultScanner.next()) {
//        	 Integer majorid = resultScanner.getInt("majorid");
            Integer deptid = resultScanner.getInt("deptid");
            Integer did = resultScanner.getInt("did");
            String dname = resultScanner.getString("dname");
            String title = resultScanner.getString("title");
//            String prof = resultScanner.getString("prof");
//            System.out.println(sname + "\t" + prof);
            System.out.println(deptid + "\t" + did + "\t" + dname + "\t" + title);
         }

         System.out.println("Query was a success");
         resultScanner.close();
         myTransaction.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}

