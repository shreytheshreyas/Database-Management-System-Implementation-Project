package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {
    @Override
    public void processFirst(Scan s) {

    }

    @Override
    public void processNext(Scan s) {

    }

    @Override
    public String fieldName() {
        return null;
    }

    @Override
    public Constant value() {
        return null;
    }
}
