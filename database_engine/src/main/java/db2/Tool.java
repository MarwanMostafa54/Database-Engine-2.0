package db2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;

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

    public static void serializeTable(Table T) {
		//store into file (serialize)
		try {
			String path =  "data//" + "table_" + T.getTableName() + ".ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]","");
			File file = new File(path); 
			FileOutputStream fileAccess;
			fileAccess = new FileOutputStream(file);
			ObjectOutputStream objectAccess = new ObjectOutputStream(fileAccess);
			objectAccess.writeObject(T);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to serialize table.");
		}
	}
    public static void WriteInFile(Hashtable<String, String> htblColNameType, String strTableName,
    String strClusteringKeyColumn) {
    String filePath = "./metadata.csv"; 
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
        for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        boolean flag = key.equals(strClusteringKeyColumn);
        writer.write(strTableName + ',' + key + ',' + value + ',' + flag + ",null,null");
        writer.newLine(); 
    }

    } catch (Exception E) {
     System.out.println("Failed to update metadata.csv!");
    E.printStackTrace();
    }
    }

}
