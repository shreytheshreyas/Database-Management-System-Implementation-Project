package simpledb.multibuffer;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class ChunkPlan implements Plan {
    private Transaction tx;
    private Plan p;
    private String tblName;
    private String fldname;
    private Layout layout;
    private Schema sch = new Schema();

    public ChunkPlan(Transaction tx, Plan p, String tblName ,String fldName, Layout layout) {
        this.tx = tx;
        this.tblName = tblName;
        this.p = p;
        this.fldname = fldName;
        this.layout = layout;
        sch.addAll(p.schema());
    }

    @Override
    public Scan open() {
        String fileName = tblName + ".tbl";
        return new ChunkScan(tx, fileName, layout, 0, tx.availableBuffs() - 2);
    }

    @Override
    public int blocksAccessed() {
        return 0;
    }

    @Override
    public int recordsOutput() {
        return 0;
    }

    @Override
    public int distinctValues(String fldname) {
        return 0;
    }

    @Override
    public Schema schema() {
        return null;
    }
}
