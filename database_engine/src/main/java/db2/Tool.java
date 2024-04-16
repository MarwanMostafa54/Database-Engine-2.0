package db2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Tool {
    public static void initializeMetaData() {
        File metadata = new File("data//metadata.csv");
        if(!metadata.exists()){
        if (metadata.length() == 0) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(metadata, true))) {
                writer.println("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Problem with writing table info");
            }
        }
        }
    }
}
