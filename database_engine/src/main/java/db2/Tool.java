package db2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class Tool {
    public static void initializeMetaData() {
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            // Create the data directory if it doesn't exist
            if (!dataDir.mkdirs()) {
                System.err.println("Failed to create data directory.");
                return;
            }
        }

        File metadata = new File(dataDir, "metadata.csv");
        if (!metadata.exists()) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(metadata))) {
                writer.println("Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType");
                System.out.println("Metadata file created successfully.");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Problem with writing table info.");
            }
        }
    }

    public static void initializeProperties() {
        File configFile = new File("config/DBApp.properties");
        if (configFile.exists()) {
            return; // Properties file already exists
        }

        Properties properties = new Properties();
        properties.setProperty("MaximumRowsCountinPage", "5");
        properties.setProperty("NodeSize", "5");
        properties.setProperty("PageId", "1");
        properties.setProperty("nextBPTID", "1");

        try {
            // Create parent directories if they don't exist
            configFile.getParentFile().mkdirs();

            // Write properties to file
            properties.store(new FileWriter(configFile), "Database engine properties");
            System.out.println("Properties file created successfully.");
        } catch (IOException e) {
            System.out.println("Failed to write properties file: " + e.getMessage());
        }
    }

    public static void serializeTable(Table T) {
        try {

            String path = "Tables/" + T.getTableName() + "/" + T.getTableName() + "_Properties" + ".ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]", "");
            File file = new File(path);
            FileOutputStream fileAccess;
            fileAccess = new FileOutputStream(file);
            ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
            objectAccess.writeObject(T);
            objectAccess.close();
            fileAccess.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to serialize table.");
        }
    }

    public static void serializePage(Table T, Page P) {
        try {
            String path = "Tables/" + T.getTableName() + "/" + T.getTableName() + T.getPageCount() + ".ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]", "");
            File file = new File(path);
            FileOutputStream fileAccess;
            fileAccess = new FileOutputStream(file);
            ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
            objectAccess.writeObject(P);
            objectAccess.close();
            fileAccess.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to serialize table.");
        }
    }

    public static Page deserializePage(Table T, int pageNumber) {
        Page page = null;
        try {
            String path = "Tables/" + T.getTableName() + "/" + T.getTableName() + pageNumber + ".ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]", "");
            FileInputStream fileAccess = new FileInputStream(path);
            ObjectInputStream objectAccess = new ObjectInputStream(fileAccess);
            page = (Page) objectAccess.readObject();
            objectAccess.close();
            fileAccess.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to deserialize page.");
        }
        return page;
    }

    public static void WriteInFile(Hashtable<String, String> htblColNameType, String strTableName,
            String strClusteringKeyColumn) {
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            // Create the data directory if it doesn't exist
            if (!dataDir.mkdirs()) {
                System.err.println("Failed to create data directory.");
                return;
            }
        }

        String filePath = "data/metadata.csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                boolean flag = key.equals(strClusteringKeyColumn);
                writer.write(strTableName + ',' + key + ',' + value + ',' + flag + ",null,null");
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Failed to update metadata.csv!");
            e.printStackTrace();
        }
    }

    public static int readPageSize(String path) {
        try {
            FileReader reader = new FileReader(path);
            Properties p = new Properties();
            p.load(reader);
            String theNum = p.getProperty("MaximumRowsCountinPage");
            return Integer.parseInt(theNum);
        }

        catch (IOException E) {
            E.printStackTrace();
            System.out.println("Error reading properties");
        }
        return 0;
    }

    public static boolean isTableUnique(String strTableName) throws DBAppException {
        try (BufferedReader reader = new BufferedReader(new FileReader("data//metadata.csv"))) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                if (isFirstLine) { // Skip the first line (header)
                    isFirstLine = false;
                    continue;
                }

                String[] data = line.split(",");
                String existingTableName = data[0];

                if (existingTableName.equals(strTableName)) {
                    return false;
                }
            }

            return true;
        } catch (FileNotFoundException e) {
            System.out.println("Metadata file cannot be located.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Error reading from Metadata file.");
            e.printStackTrace();
        }

        return false;
    }

    public static boolean checkKey(String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) {
        return htblColNameType.containsKey(strClusteringKeyColumn);
    }

    public static boolean checkApplicable(String ClassType) {
        Vector<String> datatype = new Vector<String>();
        datatype.add("java.lang.Integer");
        datatype.add("java.lang.String");
        datatype.add("java.lang.Double");
        String classTypeLower = ClassType.toLowerCase();
        for (String possible : datatype) {
            if ((possible.toLowerCase()).equals(classTypeLower)) {
                return true;
            }
        }
        return false;
    }

    public static Table deserializeTable(String tableName) {
        Table table = null;
        try {
            String path = "Tables/" + tableName + "/" + tableName + "_Properties.ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]", "");
            FileInputStream fileAccess = new FileInputStream(path);
            ObjectInputStream objectAccess = new ObjectInputStream(fileAccess);
            table = (Table) objectAccess.readObject();
            objectAccess.close();
            fileAccess.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to deserialize table.");
        }
        return table;
    }

    public static ArrayList<String[]> readMetaData(String strTableName) {

        try {
            String line = "";
            ArrayList<String[]> metaData = new ArrayList<String[]>();

            BufferedReader file = new BufferedReader(new FileReader("data//metadata.csv"));
            while ((line = file.readLine()) != null) {
                String[] data = line.split(",");
                if (data[0].equals(strTableName)) {
                    metaData.add(data);
                }
            }
            file.close();
            return metaData;
        } catch (Exception E) {
            System.out.println("Table non existent");
            return null;
        }
    }

    public static Hashtable<String, String> gethtblColNameType(ArrayList<String[]> metaDataTable) {
        Hashtable<String, String> columnNameAndType = new Hashtable<String, String>();
        for (String[] column : metaDataTable) {
            String columnName = column[1];
            String columnType = column[2];
            columnNameAndType.put(columnName, columnType);
        }
        return columnNameAndType;
    }

    public static String[] determineClusteringKey(ArrayList<String[]> metaDataTable) {
        String[] indexAndValue = new String[2];
        for (int i = 0; i < metaDataTable.size(); i++) {
            String[] array = metaDataTable.get(i);
            if (array[3].equals("True")) {
                indexAndValue[0] = array[1];
                indexAndValue[1] = i + "";
            }
        }
        return indexAndValue;
    }

    public static int readBtreeOrder(String path) {
        try {
            FileReader reader = new FileReader(path);
            Properties p = new Properties();
            p.load(reader);
            String theNum = p.getProperty("NodeSize");
            return Integer.parseInt(theNum);
        }

        catch (IOException E) {
            E.printStackTrace();
            System.out.println("Error reading properties");
        }
        return 0;
    }

    public static boolean checker(ArrayList<String[]> metaData, String strColName) {
        for (String[] data : metaData) {
            if (data[2].equals(strColName)) {
                return true; // Column name exists in metadata
            }
        }
        return false; // Column name does not exist in metadata
    }

    public static void updateMetaData(String strTableName, String indxCol, String strIndexName) {
        ArrayList<String[]> metaData = new ArrayList<String[]>();
        try {
            String line = "";

            BufferedReader read = new BufferedReader(new FileReader("data//metadata.csv"));
            while ((line = read.readLine()) != null) {
                String[] data = line.split(",");
                if (data[0].equals(strTableName) && data[1].equals(indxCol)) {
                    data[4] = strIndexName;
                    data[5] = "Btree";
                }
                metaData.add(data);
            }
            read.close();
        }

        catch (Exception E) {
            System.out.println("Failed to read from metadata.csv!");
        }
        try {
            PrintWriter write = new PrintWriter(new FileWriter("data//metadata.csv", false));
            write.append("Table Name");
            write.append(",");
            write.append("Column Name");
            write.append(",");
            write.append("Column Type");
            write.append(",");
            write.append("ClusteringKey");
            write.append(",");
            write.append("IndexName");
            write.append(",");
            write.append("IndexType");
            write.append("\n");
            write.flush();
            write.close();
        } catch (IOException E) {
            E.printStackTrace();
            System.out.println("problem with writing table info");
        }
        try {
            PrintWriter write = new PrintWriter(new FileWriter("data//metadata.csv", true));
            for (int i = 1; i < metaData.size(); i++) {
                String[] temp = metaData.get(i);
                write.append(temp[0]);
                write.append(",");

                write.append(temp[1]);
                write.append(",");

                write.append(temp[2]);
                write.append(",");

                write.append(temp[3]);
                write.append(",");

                write.append(temp[4]);
                write.append(",");

                write.append(temp[5]);
                write.append("\n");
            }
            write.flush();
            write.close();
        }

        catch (Exception E) {
            System.out.println("Failed to update metadata.csv!");
            E.printStackTrace();
        }
    }

    public static Set<String> getColumNameFromMetaData(String tableName) throws IOException {
        Set<String> ColumName = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2 && parts[0].equals(tableName)) {
                    ColumName.add(parts[1]);
                }
            }
        }
        return ColumName;
    }

    public static ArrayList<Tuple> filterTuplesByOperator(Table table, String columnName, String operator,
            Object value) {
        ArrayList<Tuple> resultTuples = new ArrayList<>();

        for (int pageId = 1; pageId <= table.getPageCount(); pageId++) {
            Page page = Tool.deserializePage(table, pageId);

            for (Tuple tuple : page.getTuples()) {
                String columnValue = tuple.getValue(columnName);

                boolean conditionSatisfied = checkCondition(columnValue, operator, value);

                if (conditionSatisfied) {
                    resultTuples.add(tuple);
                }
            }
            serializePage(table, page);
        }

        return resultTuples;
    }

    public static boolean checkCondition(String columnValue, String operator, Object value) throws IllegalArgumentException {
        // if( columnValue != value)
        //     throw new IllegalArgumentException("Invalid argument" + value); 
        switch (operator) {
            case "=":
            if (value instanceof String) {
                return columnValue.equals(value.toString());                
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue == numericValue;
            }
            case "!=":
            if (value instanceof String) {
                return !columnValue.equals(value.toString());                
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue != numericValue;
            }
            case ">":
            if (value instanceof String) {
                return columnValue.compareTo(value.toString()) > 0;               
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue > numericValue;
            }
            case ">=":
            if (value instanceof String) {
                return columnValue.compareTo(value.toString()) >= 0;               
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue >= numericValue;
            }
            case "<":
            if (value instanceof String) {
                return columnValue.compareTo(value.toString()) < 0;               
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue < numericValue;
            }
            case "<=":
            if (value instanceof String) {
                return columnValue.compareTo(value.toString()) <= 0;               
            } else {
                double columnNumericValue = Double.parseDouble(columnValue);
                double numericValue = Double.parseDouble(value.toString());
                return columnNumericValue <= numericValue;
            }
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    
    }

    //STILL DONT KNOW ITS USE
    public static void insertIntoBtree(){

    }

    //Checks if duplicate has already a hashtable inside hashtable if not then i create a new hashtable for my key
    public static boolean duplicatesExists(String strColName,int Key,Hashtable<String,Hashtable<Integer,Vector<Double>>> duplicates){
        if(duplicates.containsKey(strColName)){
            if(duplicates.get(strColName).containsKey(Key)){
                return true;
            }
        }
        return false;
    }

  //MY MATH FUNCTION TO CREATE A ENCODER VALUE
  public static double encoder(int pageID,int tupleID){
    double calculate=tupleID/(double)(Tool.readPageSize("config//DBApp.properties")+1);
    calculate+=pageID;
    return calculate;
}

//TO EXTRACT PAGE ID,TUPLE ID IN A ARRAYLIST FROM ENCODED VALUE
public static ArrayList<Integer> decoder(Double value){
    ArrayList<Integer> decode=new ArrayList<Integer>();
    int temp = (int) Math.floor(value); // Round down the double value to the nearest integer
    decode.add(temp);
    double decimalPart = value - Math.floor(value);
    int Temptuple=(int) (decimalPart*(Tool.readPageSize("config//DBApp.properties")+1));
    decode.add(Temptuple);
    return decode;

}

public static boolean CheckType(Hashtable<String, Object> htblColNameValue, Table table){
    ArrayList<String[]> metaData=Tool.readMetaData(table.getTableName());
    for(String[] item:metaData){
        Object temp=htblColNameValue.get(item[1]);
        if (temp == null) {
            return false;
        }
        switch(item[2].toLowerCase()){
             case "java.lang.string":
                    if(!(temp instanceof String)){
                        return false;
                    }
             break;

             case "java.lang.double":
             if(!(temp instanceof Integer|| temp instanceof Double)){
                return false;
             }
             break;

             case "java.lang.integer":
             if(!(temp instanceof Integer)){
                return false;
             }
             break;
        }
    }
    return true;
}
public String printRange(Table t, String columnName, bplustree tree, ArrayList<Double> pageCode) throws IOException{
    StringBuilder tuples = new StringBuilder(); 
  
    for (Double code : pageCode) {
               
        ArrayList<Integer> temp = Tool.decoder(code); 
        Page p = deserializePage(t, temp.get(0));
        Tuple tuple = p.getTuple(temp.get(1));
        tuples.append(tuple.toString()); 
        serializePage(t, p);   
    }
    if(t.duplicates.containsKey(columnName) ){
        Hashtable<Integer,Vector<Double>> duplicate = t.duplicates.get(columnName);
        Set<Map.Entry<Integer,Vector<Double>>> entrySet = duplicate.entrySet();

    for (Map.Entry<Integer,Vector<Double>> entry : entrySet) {
        Integer key = entry.getKey();
        Vector<Double> value = entry.getValue();
        if(tree.search(key) != null  && pageCode.contains(tree.search(key))) 
            for (Double v : value){
                
                ArrayList<Integer> temp2 = Tool.decoder(v);
                Page p = deserializePage(t, temp2.get(0));
                Tuple tuple = p.getTuple(temp2.get(1));
                tuples.append(tuple.toString()); 
                serializePage(t, p); 
            }
        }
            
    }
    return tuples.toString(); // Return the concatenated string
}


}
