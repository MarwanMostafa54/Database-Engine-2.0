package db2;

import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;

public class Table implements Serializable {
    public String tableName;
    public String tableMetaPath;
    public String tablePath;
    public String clusterKey;
    public Hashtable<String, bplustree> columns;
    public int pageCount;
    
    public Table(String strTableName, String clusteringKey, Hashtable<String, String> htblColNameType, String MetaPath){
        this.columns = new Hashtable<>();
        this.tableName = strTableName;
        this.clusterKey = clusteringKey;
        this.pageCount = 0;
        this.tablePath = "./Tables/" + strTableName;
        File f = new File(this.tablePath);
        if(f.exists()){
            f.mkdir();
    }
}

    public String getTableName() {
        return tableName;
    }

    public String getTableMetaPath() {
        return tableMetaPath;
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getClusterKey() {
        return clusterKey;
    }

    public Hashtable<String, bplustree> getColumns() {
        return columns;
    }

    public int getPageCount() {
        return pageCount;
    }


}
