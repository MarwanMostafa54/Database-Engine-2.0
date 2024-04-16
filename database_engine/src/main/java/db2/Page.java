package db2;

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
    
}
