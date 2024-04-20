package db2;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    private Hashtable<String, String> tuple;
    private Hashtable<String,Object> Hashtable ;
    private String hashValue;
    private double tupleID;

    public Tuple() {
        this.tuple = new Hashtable<>();
        this.Hashtable=new Hashtable<>();
    }

    public Tuple(Hashtable<String, Object> tuple, double tupleID) {
        this.tuple = new Hashtable<>();
        this.Hashtable=new Hashtable<>();  
        for (String key : tuple.keySet()) {
            this.tuple.put(key, tuple.get(key).toString());
            this.Hashtable.put(key,tuple.get(key));
        }
        this.tupleID=tupleID;
        //this.hashValue = hashValue;
    }

    public void updateTuple(Hashtable<String, Object> newValues, String clusteringKey) {

         // Store the old value of the clustering key
    String oldClusteringKeyValue = tuple.get(clusteringKey);
   // Object oldClusteringKeyObject = Hashtable.get(clusteringKey);

    // Restore the old value of the clustering key
    tuple.put(clusteringKey, oldClusteringKeyValue);
   // Hashtable.put(clusteringKey, oldClusteringKeyObject);

    // Iterate over the existing tuple entries and update their values
    for (String key : tuple.keySet()) {
        // Exclude the clustering key from the update
        if (!key.equals(clusteringKey)) {
            if (newValues.containsKey(key)) {
                Object newValue = newValues.get(key);
                tuple.put(key, newValue.toString());
              //Hashtable.put(key, newValue);
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

    public double getTupleID(){
        return this.tupleID;
    }

    public String getValue(String column) {
        return tuple.getOrDefault(column, null);
    }

    public void setValue(String columnName, String value) {
        tuple.put(columnName, value);
    }

    @Override
    public int hashCode() {
            if (hashValue != null && tuple.containsKey(hashValue)) {
                return tuple.get(hashValue).hashCode();
            } else {
                // Return a default hash code if hashValue is null or not found in the tuple
                return super.hashCode();
            }
        }

    @Override
    public boolean equals(Object obj) {
        return this.hashCode() == obj.hashCode();
    }
    public String toString() {
        if(this==null){
            return "";
        }
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
