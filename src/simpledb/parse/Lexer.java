package simpledb.parse;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.CountFn;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   private Collection<String> aggregateFunctions;
   private List<AggregationFn> aggregateFunctionFields;
   private StreamTokenizer tok;
   private Collection<String> newOperators;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initNewOperators();
      initAggregateFunctions();
      aggregateFunctionFields = new ArrayList<>();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.lowerCaseMode(true); //ids and keywords are converted
      tok.wordChars('!', '!');
      tok.wordChars(60, 62);
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
   }
   
   public boolean matchOpr() {
	   return newOperators.contains(tok.sval);
   }
   
   public String eatOpr() {
	   if(!matchOpr()) {
		   throw new BadSyntaxException();
	   }
	   String s = tok.sval;
	   nextToken();
	   return s;
   }

   public boolean matchAggregateFunction() {
      return  tok.ttype==StreamTokenizer.TT_WORD && aggregateFunctions.contains(tok.sval);
   }
//Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }

   public String eatOrderKeyword() {
      String s = tok.sval;
      if (!matchKeyword(s))
         throw new BadSyntaxException();
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not 
    * an identifier. 
    * Otherwise, returns the identifier string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }

   public String eatAggregateFunction() {
      String s = tok.sval;
      if (!matchAggregateFunction())
         throw new BadSyntaxException();
      nextToken();
      return s;
   }

   public void addAggregateFunctionField(AggregationFn function) {
      aggregateFunctionFields.add(function);
   }

   public List<AggregationFn> getAggregateFunctionFields() {
      return aggregateFunctionFields;
   }

   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }
   
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index", "on",
                               "using", "hash", "btree", "order", "by", "asc", "desc", "inner", "join", "group",
                               "count", "max", "min", "sum", "avg");
   }

   private void initAggregateFunctions() {
      aggregateFunctions = Arrays.asList("max", "min", "count", "sum", "avg");
   }
   
   private void initNewOperators() {
	   newOperators = Arrays.asList("=", "<", "<=", ">", ">=", "!=", "<>");
   }
}
