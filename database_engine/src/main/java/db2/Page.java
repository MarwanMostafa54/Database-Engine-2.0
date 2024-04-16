package db2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
    private Vector<Tuple> tuples;
    private int tupleId;
    private int N;
	private int PageId;
    public Page(int PageId){
        tupleId=1;
        tuples = new Vector<Tuple>();
        this.PageId=PageId;
        N = Tool.readPageSize("config//DBApp.properties");
    }
    
    public int getN() {
        return N;
    }
    public int getPageID() {
        return PageId;
    }
    
    public int getTupleID() {
        return tupleId;
    }

    public Vector<Tuple> getTuples()
	{
		return this.tuples;
	}

    public boolean isEmpty() {
		return tuples.size() <= 0;
    }

	public boolean isFull() {
		return tuples.size() >N;
    }
    
    public int AddTuple(Tuple tuple)
	{
		tuples.add(tuple);
		tupleId++;
		return tupleId;
	}

    public Tuple getTuple(int index) throws IOException
	{
		return tuples.get(index);
	}

	public void deleteTuple(int index)
	{
		tuples.remove(index);
		tupleId--;
	}

	


}
