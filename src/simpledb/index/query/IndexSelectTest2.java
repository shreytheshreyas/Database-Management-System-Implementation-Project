package simpledb.index.query;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.index.*;
import simpledb.index.planner.IndexSelectPlan;

import java.util.Map;
import simpledb.record.*;

// Obtaining department id of course.

public class IndexSelectTest2 {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();

        // Find the index on title of the course relation.
        Map<String,IndexInfo> indexes = mdm.getIndexInfo("course", tx);
        IndexInfo titleIndex = indexes.get("title");

        // Get the plan for the Course table
        Plan courseRelationPlan = new TablePlan(tx, "course", mdm);

        // Create the selection constant
        Constant c = new Constant("db systems");

        // Two different ways to use the index in simpledb:
		useIndexManually(titleIndex, courseRelationPlan, c);
//        useIndexScan(titleIndex, courseRelationPlan, c);

        tx.commit();
    }

    private static void useIndexManually(IndexInfo ii, Plan p, Constant c) {
        // Open a scan on the table.
        TableScan s = (TableScan) p.open();  //must be a table scan
        Index idx = ii.open(); //returns the respective index based on idx type

        // Retrieve all index records having the specified dataval.
        idx.beforeFirst(c);
        while (idx.next()) {
            // Use the datarid to go to the corresponding course record.
            RID datarid = idx.getDataRid();
            s.moveToRid(datarid);  // table scans can move to a specified RID.
            System.out.println(s.getInt("deptid"));
        }
        idx.close();
        s.close();
    }

    private static void useIndexScan(IndexInfo ii, Plan p, Constant c) {
        // Open an index select scan on the enroll table.
        Plan idxplan = new IndexSelectPlan(p, ii, c);
        Scan s = idxplan.open();

        while (s.next()) {
            System.out.println(s.getInt("deptid"));
        }
        s.close();
    }
}
