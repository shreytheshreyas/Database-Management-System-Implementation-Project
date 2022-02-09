package simpledb.test;

import simpledb.plan.Planner;
import simpledb.tx.Transaction;
import simpledb.server.SimpleDB;


public class CreateStudentDB {
   public static void main(String[] args) {
      try{
         //Creating Database
         SimpleDB myDatabase = new SimpleDB("studentdb");
         //Creating Transaction
         Transaction myTransaction = myDatabase.newTx();
         //Creating Planner
         Planner myPlanner = myDatabase.planner();

         //CREATING STUDENT TABLE
         String createTableStatement = "create table STUDENT(sid int, sname varchar(10), majorid int, gradyear int)";
         myPlanner.executeUpdate(createTableStatement, myTransaction);
         System.out.println("Table STUDENT created.");

         //Creating Index on Student's realtion on the majorid field.
         String createIndexStatement = "create index myStudentIndex on student (majorid) using btree";
         myPlanner.executeUpdate(createIndexStatement, myTransaction);
         System.out.println("Index for MajorId created on Student table");

         //INSERTING STUDENT DATA
         String insertDataStatement = "insert into STUDENT(sid, sname, majorid, gradyear) values ";
         String[] studentValues = {"(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
         "(9, 'lee', 10, 2021)"};
         for (String studentValue : studentValues) {
            myPlanner.executeUpdate(insertDataStatement + studentValue, myTransaction);
         }
         System.out.println("STUDENT records inserted.");

         //CREATING DEPARTMENTS TABLE
         createTableStatement = "create table DEPT(did int, dname varchar(8))";
         myPlanner.executeUpdate(createTableStatement,myTransaction);
         System.out.println("Table DEPT created.");

         //INSERTING DEPARTMENTS DATA
         insertDataStatement = "insert into DEPT(did, dname) values ";
         String[] departmentValues = {"(10, 'compsci')",
                              "(20, 'math')",
                              "(30, 'drama')"};
         for (String departmentValue : departmentValues) {
            String command = insertDataStatement + departmentValue;
            myPlanner.executeUpdate(command, myTransaction);
         }
         System.out.println("DEPT records inserted.");

         //CREATING COURSE TABLE
         createTableStatement = "create table COURSE(cid int, title varchar(20), deptid int)";
         myPlanner.executeUpdate(createTableStatement, myTransaction);
         System.out.println("Table COURSE created.");

         //CREATING COURSES TABLE INDEX ON COURSES' RELATION ON THE TITLE FIELD
         createIndexStatement = "create index myCoursesIndex on course(title)";
         myPlanner.executeUpdate(createIndexStatement, myTransaction);
         System.out.println("Index for course title has been created");

         //INSERTING COURSES DATA
         insertDataStatement = "insert into COURSE(cid, title, deptid) values ";
         String[] courseValues = {"(12, 'db systems', 10)",
                                "(22, 'compilers', 10)",
                                "(32, 'calculus', 20)",
                                "(42, 'algebra', 20)",
                                "(52, 'acting', 30)",
                                "(62, 'elocution', 30)"};
         for (String courseValue : courseValues) {
            String command = insertDataStatement + courseValue;
            myPlanner.executeUpdate(command, myTransaction);
         }
         System.out.println("COURSE records inserted.");

         //CREATING SECTIONS TABLE
         createTableStatement = "create table SECTION(sectid int, courseid int, prof varchar(8), yearoffered int)";
         myPlanner.executeUpdate(createTableStatement, myTransaction);
         System.out.println("Table SECTION created.");

         //CREATING COURSES TABLE INDEX ON SECTION'S RELATION ON THE TITLE FIELD
         createIndexStatement = "create index mySectionIndex on section (courseid)";
         myPlanner.executeUpdate(createIndexStatement, myTransaction);
         System.out.println("Index for section courseid has been created");

         //INSERTING STUDENT DATA
         insertDataStatement = "insert into SECTION(sectid, courseid, prof, yearoffered) values ";
         String[] sectionValues = {"(13, 12, 'turing', 2018)",
                              "(23, 12, 'turing', 2019)",
                              "(33, 32, 'newton', 2019)",
                              "(43, 32, 'einstein', 2017)",
                              "(53, 62, 'brando', 2018)"};
         for (String sectionValue : sectionValues) {
            String command = insertDataStatement + sectionValue;
            myPlanner.executeUpdate(command, myTransaction);
         }
         System.out.println("SECTION records inserted.");

         //CREATING ENROLLS TABLE
         createTableStatement = "create table ENROLL(eid int, studentid int, sectionid int, grade varchar(2))";
         myPlanner.executeUpdate(createTableStatement, myTransaction);
         System.out.println("Table ENROLL created.");

         //CREATING INDEX ON ENROLL'S RELATION ON THE SID FIELD
         createIndexStatement = "create index myEnrollsIndex on enroll (studentid) using hash";
         myPlanner.executeUpdate(createIndexStatement, myTransaction);
         System.out.println("Index for StudentId created on Enroll table");

         //INSERTING ENROLLS DATA
         insertDataStatement = "insert into ENROLL(eid, studentid, sectionid, grade) values ";
         String[] enrollValues = {"(14, 1, 13, 'A')",
                                "(24, 1, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(44, 4, 33, 'B' )",
                                "(54, 4, 53, 'A' )",
                                "(64, 6, 53, 'A' )"};
         for (String enrollValue : enrollValues) {
            String command = insertDataStatement + enrollValue;
            myPlanner.executeUpdate(command, myTransaction);
         }
         System.out.println("ENROLL records inserted.");
         myTransaction.commit();

      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
