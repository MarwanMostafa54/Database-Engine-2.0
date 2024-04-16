package db2;

import java.io.IOException;
import java.util.Vector;

public class Page {
    private Vector<Tuple> tuples;
    private int tupleId;
    private int N;
    public Page(){
        tupleId=1;
        tuples = new Vector<Tuple>();
        N = Tool.readPageSize("config//DBApp.properties");
    }
    
    public int getN() {
        return N;
    }
    
    public int getTupleID() {
        return tupleId;
    }

    public Vector<Tuple> getTuples()
	{
		return this.tuples;
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

	public void deleteTuple(int index) throws IOException
	{
		 tuples.remove(index);
		 tupleId--;
		 if(tupleId<1)
		 	Table.deletePage(this);	
	}


}
