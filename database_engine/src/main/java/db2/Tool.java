package db2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

public class Tool {
    public static void initializeMetaData() {
        File metadata = new File("data//metadata.csv");
    
        if (metadata.length() == 0) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(metadata, true))) {
                writer.println("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Problem with writing table info");
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

}
