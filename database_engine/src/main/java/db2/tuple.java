package db2;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private Hashtable<String, String> tuple;
    private String hashValue;
    private double tupleID;

    public Tuple() {
        this.tuple = new Hashtable<>();
    }

    public Tuple(Hashtable<String, Object> tuple, double tupleID) {
        this.tuple = new Hashtable<>();
        for (String key : tuple.keySet()) {
            this.tuple.put(key, tuple.get(key).toString());
        }
        this.tupleID = tupleID;
    }

    public double getTupleID(){
        return this.tupleID;
    }

    public String getValue(String column) {
        return tuple.getOrDefault(column, null);
    }

    public void setValue(String columnName, String value) {
        tuple.put(columnName, value);
    }

    public int hashCode() {
        return tuple.get(hashValue).hashCode();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String key : tuple.keySet()) {
            sb.append(tuple.get(key)).append(",");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public String getColumnValue(String columnName) {
        return tuple.get(columnName);
    }
}
