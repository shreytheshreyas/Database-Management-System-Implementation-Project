package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn {
    private String fldname;
    private Integer sumValue;
    private Integer counterValue;

    public AvgFn(String fldname) {
        this.fldname = fldname;

    }

    @Override
    public void processFirst(Scan s) {
        sumValue = s.getInt(fldname);
        counterValue = 1;
    }

    @Override
    public void processNext(Scan s) {
        Integer newval = s.getInt(fldname);
        sumValue += newval;
        counterValue += 1;
    }

    @Override
    public String fieldName() {
        return "avgof" + fldname;
    }

    @Override
    public Constant value() {
        Constant result = new Constant(sumValue / counterValue);
        return result;
    }
}
