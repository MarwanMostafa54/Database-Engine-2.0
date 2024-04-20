package db2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
    private Vector<Tuple> tuples;
    private int tupleCount;
    private int N;
    private int PageId;

    public Page(int PageId) {
        tupleCount = 0;
        tuples = new Vector<Tuple>();
        this.PageId = PageId;
        N = Tool.readPageSize("config//DBApp.properties");
    }

    public int getN() {
        return N;
    }

    public int getPageID() {
        return PageId;
    }

    public int gettupleCount() {
        return tupleCount;
    }

    public Vector<Tuple> getTuples() {
        return this.tuples;
    }

    public boolean isEmpty() {
        return tuples.size() <= 0;
    }

    public boolean isFull() {
        return tuples.size() >= N;
    }

    public int AddTuple(Tuple tuple) {
        tuples.add(tuple);
        tupleCount++;
        return tupleCount;
    }

    public Tuple getTuple(int index) throws IOException {
        return tuples.get(index - 1);
    }

    public void deleteTuple(int index) {
        tuples.set(index - 1, null);
        tupleCount--;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tuples) {
            sb.append(tuple.toString()).append(" , ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public void setPageID(int newPageID) {
        this.PageId = newPageID;
    }

}
