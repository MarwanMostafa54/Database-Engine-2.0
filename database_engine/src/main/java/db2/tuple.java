package db2;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private Hashtable<String, String> tuple;
    private Hashtable<String,Object> Hashtable;
    private String hashValue;

    public Tuple() {
        this.tuple = new Hashtable<>();
    }

    public Tuple(Hashtable<String, Object> tuple) {
        this.Hashtable=new Hashtable<>();
        for (String key : tuple.keySet()) {
            this.Hashtable.put(key, tuple.get(key));
        }
        this.tuple = new Hashtable<>();
        for (String key : tuple.keySet()) {
            this.tuple.put(key, tuple.get(key).toString());
        }
        //this.hashValue = hashValue;
    }

    public void updateTuple(Hashtable<String, Object> newValues, String clusteringKey) {

         // Store the old value of the clustering key
    String oldClusteringKeyValue = tuple.get(clusteringKey);
    Object oldClusteringKeyObject = Hashtable.get(clusteringKey);

    // Restore the old value of the clustering key
    tuple.put(clusteringKey, oldClusteringKeyValue);
    Hashtable.put(clusteringKey, oldClusteringKeyObject);

    // Iterate over the existing tuple entries and update their values
    for (String key : tuple.keySet()) {
        // Exclude the clustering key from the update
        if (!key.equals(clusteringKey)) {
            if (newValues.containsKey(key)) {
                Object newValue = newValues.get(key);
                tuple.put(key, newValue.toString());
                Hashtable.put(key, newValue);
            }
        }
        }
    }

    public Hashtable<String, String> getTupleInfo(){
        return this.tuple;
    }
    public Hashtable<String, Object> getHashtable(){
        return this.Hashtable;
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
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public String getColumnValue(String columnName) {
        return tuple.get(columnName);
    }
}
