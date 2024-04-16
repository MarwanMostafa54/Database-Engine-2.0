package db2;

import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;

public class Table implements Serializable {
    public String tableName;
    public String tableMetaPath;
    public String tablePath;
    public String clusterKey;
    public Hashtable<String, bplustree> columns;
    public int pageCount;
    
    public Table(String strTableName, String clusteringKey, Hashtable<String, String> htblColNameType, String MetaPath) throws DBAppException{
        if (!Tool.checkKey(clusteringKey, htblColNameType)) {
            throw new DBAppException("Invalid Selected Clustering Column");
        } else {
            if (Tool.isTableUnique(strTableName)) {
                Set<String> colName = htblColNameType.keySet();

                for (String n : colName) {
                    if (Tool.checkApplicable(htblColNameType.get(n)) == false) {
                        throw new DBAppException("Invalid column type.");
                    }
                }
                this.columns = new Hashtable<>();
                this.tableName = strTableName;
                 this.clusterKey = clusteringKey;
                this.pageCount = 0;
                this.tablePath = "Tables/" + strTableName;
                File dataDir = new File("Tables");
                if (!dataDir.exists()) {
                    if (!dataDir.mkdirs()) {
                        System.err.println("Failed to create data directory.");
                        return;
                    }
                }
                File f = new File(dataDir, strTableName);
                if (!f.exists()) {
                    if (!f.mkdirs()) {
                        System.err.println("Failed to create data directory.");
                        return;
                    }
                }
                Tool.serializeTable(this);
                Tool.WriteInFile(htblColNameType, strTableName, clusteringKey);
            }
            else {
                throw new DBAppException("Table Name already exists");
            }
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

    public static void deletePage(Page page) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deletePage'");
    }



}
