package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private LinkedHashMap<String, String> orderFields = new LinkedHashMap<>();
   private List<String> groupByFields;
   private List<AggregationFn> aggFunctions;
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, LinkedHashMap<String,
           String> orderFields, ArrayList<String> groupByFields, List<AggregationFn> aggFunctions) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.orderFields = orderFields;
      this.groupByFields = groupByFields;
      this.aggFunctions = aggFunctions;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }

   public boolean hasOrderFields() {
      if(orderFields != null)
         return !orderFields.isEmpty();
      else
         return false;
   }

   public boolean hasGroupByFields() {
      if(groupByFields != null)
         return !groupByFields.isEmpty();
      else
         return false;
   }

   public LinkedHashMap<String, String> orderFields() {
      return orderFields;
   }

   public List<String> getGroupByFields() {
      return groupByFields;
   }

   public List<AggregationFn> getAggFunctions() {
      return aggFunctions;
   }
}
