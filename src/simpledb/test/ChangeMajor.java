package simpledb.test;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.plan.Planner;


public class ChangeMajor {
   public static void main(String[] args) {
      try {
         //CREATE DATABASE
         SimpleDB myDatabase = new SimpleDB("studentdb");
         //INITIALIZING TRANSACTION
         Transaction myTransaction = myDatabase.newTx();
         //INITIALIZING PLANNER
         Planner myPlanner = myDatabase.planner();

         //INITIALIZING COMMAND
         String cmd = "update STUDENT "
                    + "set MajorId=30 "
                    + "where SName = 'amy'";

         myPlanner.executeUpdate(cmd, myTransaction);
         System.out.println("Amy is now a drama major.");
         myTransaction.commit();
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
