package simpledb.materialize;

import simpledb.query.*;
import java.util.*;

/**
 * The Scan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class HashScan implements Scan {

    @Override
    public void beforeFirst() {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public int getInt(String fldname) {
        return 0;
    }

    @Override
    public String getString(String fldname) {
        return null;
    }

    @Override
    public Constant getVal(String fldname) {
        return null;
    }

    @Override
    public boolean hasField(String fldname) {
        return false;
    }

    @Override
    public void close() {

    }
}
