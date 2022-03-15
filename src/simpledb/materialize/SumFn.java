package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class SumFn implements AggregationFn {
    private String fldname;
    private Integer sumValue;

    public SumFn(String fldname) {
        this.fldname = fldname;
        this.sumValue = 0;
    }

    @Override
    public void processFirst(Scan s) {
        sumValue = s.getInt(fldname);
    }

    @Override
    public void processNext(Scan s) {
        Integer newval = s.getInt(fldname);
        sumValue += newval;
    }

    @Override
    public String fieldName() {
        return "sumof" + fldname;
    }

    @Override
    public Constant value() {
        return new Constant(sumValue);
    }
}
