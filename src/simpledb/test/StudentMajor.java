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
//         String myQuery = "SELECT deptid, did, title, dname FROM dept JOIN course ON did = deptid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM student JOIN enroll ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON sid = studentid";
//         String myQuery = "SELECT sid, studentid, SName, Grade FROM enroll JOIN student ON studentid = sid";
         
         
//         String myQuery = "SELECT distinct majorid FROM student WHERE gradyear = 2022 or sid = 3";
//         String myQuery = "select distinct sname from student, enroll where sid = studentid order by sname desc";
//         String myQuery = "SELECT count(sid), max(sid), gradyear, min(sid), sum(sid), avg(sid) FROM student GROUP BY gradyear";
         
         String myQuery = "SELECT sid, sname, eid, cid FROM student, course, enroll WHERE sid = studentid";
         
         //Scanning result set

         //GROUP BY QUERY TEST
//         String myQuery = "SELECT deptid, max(cid) FROM COURSE GROUP BY deptid";

         //Creating a query Plan
         //String myQuery = "SELECT sid FROM STUDENT sid > 8";
         Plan myPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myPlan.open();
//         System.out.println(myPlan);
         System.out.println("sid\tsname\teid\tcid");
         while (resultScanner.next()) {
//            Integer deptid = resultScanner.getInt("deptid");
//            Integer did = resultScanner.getInt("did");
//            String title = resultScanner.getString("title");
//            String dname = resultScanner.getString("dname");

            Integer sid = resultScanner.getInt("sid");
//            Integer studentid = resultScanner.getInt("studentid");
            String name = resultScanner.getString("sname");
            Integer eid = resultScanner.getInt("eid");
            Integer cid = resultScanner.getInt("cid");
//            String grade = resultScanner.getString("grade");
            System.out.println(sid+ "\t\t\t\t"+ name + "\t\t\t\t" + eid + "\t\t\t\t" + cid);
//            System.out.println(name);

            //Group By fields
            Integer sid_count = resultScanner.getInt("countofsid");
            Integer sid_max = resultScanner.getInt("maxofsid");
            Integer gradyear = resultScanner.getInt("gradyear");
            Integer sid_min = resultScanner.getInt("minofsid");
            Integer sid_sum = resultScanner.getInt("sumofsid");
            Integer sid_avg = resultScanner.getInt("avgofsid");
//            System.out.println(sid_count + "\t" + sid_max + "\t" + sid_min + "\t" + sid_sum + "\t" + sid_avg + "\t" + title_count);
            System.out.println(sid_count + "\t" + sid_max + "\t" + sid_min + "\t" + gradyear + "\t" + sid_sum + "\t" + sid_avg + "\t");
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

