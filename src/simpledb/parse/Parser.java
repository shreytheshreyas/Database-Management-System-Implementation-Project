package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   
   public Parser(String s) {
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
      return lex.eatId();
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
//      ORIGINAL IMPLEMENTATION
//      lex.eatDelim('=');

//      MY IMPLEMENTATION
      String relationalOp = "=";

      if (lex.matchDelim('=')) {
         lex.eatDelim('=');
      } else if (lex.matchDelim('<')) {
         lex.eatDelim('<');
         relationalOp = "<";
         if (lex.matchDelim('=')) {
            lex.eatDelim('=');
            relationalOp = "<=";
         }
      } else if (lex.matchDelim('>')) {
         lex.eatDelim('>');
         relationalOp = ">";
         if (lex.matchDelim('=')) {
            lex.eatDelim('=');
            relationalOp = ">=";
         }
      } else if (lex.matchDelim('!')) {
         lex.eatDelim('!');
         if (lex.matchDelim('=')) {
            lex.eatDelim('=');
            relationalOp = "!=";
         } else {
            //THROW EXCEPTION FOR INVALID OPERATION 1
            System.out.println("The specified operator does not exist");
            System.exit(0);
         }
      } else {
         System.out.println("The specified operator does not exist");
         System.exit(0);
      }

      Expression rhs = expression();
      return new Term(lhs, rhs, relationalOp);
   }

   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      lex.eatKeyword("select");
      List<String> fields = selectList();
      lex.eatKeyword("from");
      Collection<String> tables = tableList(); //Collection of Database relations

      //LOGIC FOR WHERE
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      } else if (lex.matchKeyword("on")) {
         lex.eatKeyword("on");
         pred = predicate();
      }

      //LOGIC FOR GROUP BY
      ArrayList<String> groupByFields = null;
      if(lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         if(lex.matchKeyword("by")) {
            lex.eatKeyword("by");
            groupByFields = groupFieldList();
         }
      }

      //LOGIC FOR ORDER BY
      LinkedHashMap<String, String> orderFields = null;
      if(lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         if (lex.matchKeyword("by")) {
            lex.eatKeyword("by");
            orderFields = orderList();
         }
      }
      return new QueryData(fields, tables, pred, orderFields, groupByFields);
   }
   
   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      //need to add code to deal with aggregate function
      if(lex.matchKeyword("count")) {
         lex.eatKeyword("count");
         lex.eatDelim('(');
         L.add(field());
         lex.eatDelim(')');
      } else if(lex.matchKeyword("max")) {
         lex.eatKeyword("max");
         lex.eatDelim('(');
         L.add(field());
         lex.eatDelim(')');
      } else if(lex.matchKeyword("min")) {
         lex.eatKeyword("min");
         lex.eatDelim('(');
         L.add(field());
         lex.eatDelim(')');
      } else if(lex.matchKeyword("sum")) {
         lex.eatKeyword("sum");
         lex.eatDelim('(');
         L.add(field());
         lex.eatDelim(')');
      } else if(lex.matchKeyword("avg")) {
         lex.eatKeyword("avg");
         lex.eatDelim('(');
         L.add(field());
         lex.eatDelim(')');
      } else {
         L.add(field());
      }


      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      } else if(lex.matchKeyword("join")) {
         lex.eatKeyword("join");
         L.addAll(selectList());
      }
      return L;
   }

   private LinkedHashMap<String, String> orderList() {
      LinkedHashMap<String, String> myMap = new LinkedHashMap<>();
      String primaryField = field();
      String order = (lex.matchKeyword("asc") || lex.matchKeyword("desc")) ? lex.eatOrderKeyword() : "asc";
      myMap.put(primaryField, order);

      while(lex.matchDelim(',')) {
         lex.eatDelim(',');
         String subField = field();
         String subOrder = (lex.matchKeyword("asc") || lex.matchKeyword("desc")) ? lex.eatOrderKeyword() : "asc";
         myMap.put(subField, subOrder);
      }
//      if (lex.matchDelim(',')) {
//         lex.eatDelim(',');
//         myMap.putAll(orderList());
//      }
      return myMap;
   }

   private ArrayList<String> groupFieldList() {
      ArrayList<String> myList  = new ArrayList<>();
      myList.add(field());
      if(lex.matchDelim(',')) {
         myList.addAll(groupFieldList());
      }

      return myList;
   }

   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      } else if (lex.matchKeyword("join")) {
         lex.eatKeyword("join");
         L.addAll(tableList());
      }
      return L;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      String idxtype = null;
      if (lex.matchKeyword("using")) {
         lex.eatKeyword("using");
         if (lex.matchKeyword("hash"))
            idxtype = "hash";
         else if (lex.matchKeyword("btree"))
            idxtype = "btree";
      }
      else {
         idxtype = "btree"; //WILL DEFAULT TO B+TREE INDEX IMPLEMENTATION IF THE USING KEYWORD IS NOT SPECIFIED
      }
      return new CreateIndexData(idxname, tblname, fldname, idxtype);
//      return new CreateIndexData(idxname, tblname, fldname, "btree");
   }
}

