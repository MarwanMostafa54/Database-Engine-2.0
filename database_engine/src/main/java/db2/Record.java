package db2;
import java.io.Serializable;
import java.util.Hashtable;
public class Record implements Serializable {
    private Hashtable<String,String> record;
    private String hashValue;
    public Record(){
        this.record = new Hashtable<>();
    }

public Record(Hashtable<String, Object> record,String hashValue){
    this.record = new Hashtable<>();
    for (String key : record.keySet())
        this.record.put(key, record.get(key).toString());
        this.hashValue = hashValue;
    
}
public String getValue(String column){
    return record.getOrDefault(column, null);
}

public int hashCode(){
    return record.get(hashValue).hashCode();
}

}
