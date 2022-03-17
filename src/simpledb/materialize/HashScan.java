package simpledb.materialize;

import simpledb.query.*;
import java.util.*;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class HashScan implements Scan {
   private Scan s1;
   private Scan s2;
   private String fldname1, fldname2;
   private Constant joinval = null;
   private int listIndex;
   private int noOfPartitions;
   private int partitionIndex;
   private List<String> s1Fields;   
   private Map<Constant, ArrayList<Map<String, Constant>>> s1Partition;
   private ArrayList<Map<String, Constant>> tempList;
   private Map<String, Constant> tempRecord;
   
   /**
    * Create a mergejoin scan for the two underlying sorted scans.
    * @param s1 the LHS sorted scan
    * @param s2 the RHS sorted scan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    */
   public HashScan(Scan s1, String fldname1, Scan s2, String fldname2, int noOfPartitions, List<String> s1Fields) {
      this.s1 = s1;
      this.s2 = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.s1Fields = s1Fields;
      this.noOfPartitions = noOfPartitions;
      this.listIndex = 0;
      beforeFirst();
   }
   
   /**
    * Close the scan by closing the two underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }
   
  /**
    * Position the scan before the first record,
    * by positioning each underlying scan before
    * their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
	   partitionIndex = 0;
	   s1Partitioning();
   }
   
   /**
    * Move to the next record.  This is where the action is.
    * <P>
    * If the next RHS record has the same join value,
    * then move to it.
    * Otherwise, if the next LHS record has the same join value,
    * then reposition the RHS scan back to the first record
    * having that join value.
    * Otherwise, repeatedly move the scan having the smallest
    * value until a common join value is found.
    * When one of the scans runs out of records, return false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
	   // List of s1 records that equates to current s2 record
	   if (tempList != null) {
		   if (listIndex < tempList.size()) {
			   tempRecord = tempList.get(listIndex);
			   listIndex++;
			   return true;
		   }
	   }
	   // Probing
	   while(s2.next()) {
		   int hashValue = s2.getVal(fldname2).hashCode();
		   if(hashValue % noOfPartitions == partitionIndex) {
				if(s1Partition.containsKey(s2.getVal(fldname2))) {
					listIndex = 0;
					tempList = s1Partition.get(s2.getVal(fldname2));
					tempRecord = tempList.get(listIndex);
					listIndex++;
					return true;
				}
			}
		}
	   partitionIndex++;
	   if (partitionIndex > noOfPartitions) {
		   return false;
	   }
	   s1Partitioning();
//	   if(!s1Partitioning()) {
//		   return false;
//	   }
	   return next();
   }
   
   public boolean s1Partitioning() {
	   
	   s1.beforeFirst();
	   s2.beforeFirst();
	   s1Partition = new HashMap<>();
	   
	   while(s1.next()) {
		   int hashValue = s1.getVal(fldname1).hashCode();
		   if(hashValue % noOfPartitions == partitionIndex) {
			   Map<String, Constant> s1Record = new HashMap<>();
			   for(String field: s1Fields) 
				   s1Record.put(field, s1.getVal(field));
			   
			   if (s1Partition.containsKey(s1.getVal(fldname1))) {
				   ArrayList<Map<String, Constant>> value = s1Partition.get(s1.getVal(fldname1));
				   value.add(s1Record);
				   s1Partition.replace(s1.getVal(fldname1), value);
				   
			   } else {
				   ArrayList<Map<String, Constant>> value = new ArrayList<Map<String, Constant>>();
				   value.add(s1Record);
				   s1Partition.put(s1.getVal(fldname1), value);
			   }
		   }
	   }
		
	   return true;
   }
   
   /** 
    * Return the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
	   if (s1.hasField(fldname))
			return tempRecord.get(fldname).asInt();
		else
			return s2.getInt(fldname);
   }
   
   /** 
    * Return the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
	   if (s1.hasField(fldname))
			return tempRecord.get(fldname).asString();
		else
			return s2.getString(fldname);
   }
   
   /** 
    * Return the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
	   if (s1.hasField(fldname))
			return tempRecord.get(fldname);
		else
			return s2.getVal(fldname);
   }
   
   /**
    * Return true if the specified field is in
    * either of the ucurS1Record.get(fldname)nderlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return tempRecord.containsKey(fldname) || s2.hasField(fldname);
   }
}
