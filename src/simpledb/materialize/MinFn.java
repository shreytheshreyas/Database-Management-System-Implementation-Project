package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {
    private String fldname;
    private Constant val;

    public MinFn(String fldname) {
        this.fldname = fldname;
    }

    @Override
    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    @Override
    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0)
            val = newval;
    }

    @Override
    public String fieldName() {
        return "minof" + fldname;
    }

    @Override
    public Constant value() {
        return val;
    }
}
