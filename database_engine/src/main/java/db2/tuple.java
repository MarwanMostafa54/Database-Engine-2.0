package db2;
import java.io.Serializable;
import java.util.Hashtable;
public class Tuple implements Serializable {
    private Hashtable<String,String> tuple;
    private String hashValue;
    public Tuple(){
        this.tuple = new Hashtable<>();
    }

public Tuple(Hashtable<String, Object> tuple,String hashValue) {
    this.tuple = new Hashtable<>();
    for (String key : tuple.keySet()){
        this.tuple.put(key, tuple.get(key).toString());}
    this.hashValue = hashValue;
}
public String getValue(String column){
    return tuple.getOrDefault(column, null);
}

public void setValue(String columnName, String value) {
    tuple.put(columnName, value);
}

public int hashCode(){
    return tuple.get(hashValue).hashCode();
}

public String toString(){
    StringBuilder sb = new StringBuilder();
        for (String key : tuple.keySet()) {
            sb.append(tuple.get(key)).append(",");
        }
        sb.setLength(sb.length() - 2); // Remove the last ", "
        return sb.toString();
}

}
