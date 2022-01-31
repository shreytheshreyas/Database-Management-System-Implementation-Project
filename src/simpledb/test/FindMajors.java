package simpledb.test;

import simpledb.query.Scan;
import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.plan.Plan;
import simpledb.server.SimpleDB;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;

public class FindMajors {
   public static void main(String[] args) {
      System.out.print("Enter a department name: ");
      Scanner sc = new Scanner(System.in);
      String major = sc.next();
      sc.close();


      try {
         //Initializing Database
         SimpleDB myDatabase = new SimpleDB("studentdb");
         //Creating Transaction
         Transaction myTransaction = myDatabase.newTx();
         //Creating Query Planner
         Planner myPlanner = myDatabase.planner();

         //Initializing command statement
         String myQuery = "select sname, gradyear "
               + "from student, dept "
               + "where did = majorid "
               + "and dname = '" + major + "'";

         Plan myQueryPlan = myPlanner.createQueryPlan(myQuery, myTransaction);
         Scan resultScanner = myQueryPlan.open();

         System.out.println("Here are the " + major + " majors");
         System.out.println("Name GradYear");
         while (resultScanner.next()) {
            String sname = resultScanner.getString("sname");
            int gradyear = resultScanner.getInt("gradyear");
            System.out.println(sname + "\t" + gradyear);
         }
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
