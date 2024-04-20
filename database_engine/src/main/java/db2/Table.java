package db2;

import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {
    public String tableName;
    public String tableMetaPath;
    public String tablePath;
    public String clusterKey;
    public Hashtable<String, bplustree> Indices;
    public int pageCount;
    public Hashtable<String,Hashtable<Integer,Vector<Double>>> duplicates;
    

    public Table(String strTableName, String clusteringKey, Hashtable<String, String> htblColNameType)
            throws DBAppException {
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
                this.Indices = new Hashtable<>();
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
                this.duplicates=new  Hashtable<String,Hashtable<Integer,Vector<Double>>>();
                Tool.serializeTable(this);
                Tool.WriteInFile(htblColNameType, strTableName, clusteringKey);
            } else {
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

    public Hashtable<String, bplustree> getIndices() {
        return Indices;
    }

    public void addIndex(String columnName, bplustree tree) {
        Indices.put(columnName, tree);
    }

    public int getPageCount() {
        return pageCount;
    }

    public Page CreateNewPage() {
        Tool.deserializeTable(this.getTableName());
        Page p = new Page(++pageCount);
        System.out.println("New Page Created");
        Tool.serializePage(this, p);
        Tool.serializeTable(this);
        return p;
    }

    public void deletePage(int PageId) {
        String fileName = "Tables/" + this.getTableName() + "/" + this.getTableName() + PageId + ".ser";
        Page page = Tool.deserializePage(this, PageId);
        File file = new File(fileName);
        System.out.println(file.exists());
        try {
            file.delete();
            System.out.println(fileName);
            page = null;
            pageCount--;
            System.out.println("Page Delete");
            Tool.serializeTable(this);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to delete page.");
        }
    }

    public void insertTupleIntoLastPage(Tuple tuple) {
        if(this.pageCount>0){
        Page p = Tool.deserializePage(this, this.pageCount);
        if (p.isFull()) {
            this.CreateNewPage();
            Page p1 = Tool.deserializePage(this, this.pageCount);
            p1.AddTuple(tuple);
            Tool.serializePage(this, p1);
        } else {
            p.AddTuple(tuple);
            Tool.serializePage(this, p);
        }}
        else{
            this.CreateNewPage();
            Page p1 = Tool.deserializePage(this, this.pageCount);
            p1.AddTuple(tuple);
            Tool.serializePage(this, p1);
        }
        Tool.serializeTable(this);
    }

    public void deleteTuple(int pageID, int tupleID) {
        Page p = Tool.deserializePage(this, pageID);
        p.deleteTuple(tupleID);
        if (p.isEmpty()) {
            this.deletePage(pageID);
            Tool.shiftPages(pageID,this);
        } else {
            Tool.serializePage(this, p);
        }
        Tool.serializeTable(this);
    }

}
