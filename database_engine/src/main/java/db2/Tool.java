package db2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

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

    public static int readPageSize(String path) {
		try{
			FileReader reader =new FileReader(path);
			Properties p = new Properties();
			p.load(reader);
			String theNum = p.getProperty("MaximumRowsCountinPage");
			return Integer.parseInt(theNum);}

		catch(IOException E){
			E.printStackTrace();
			System.out.println("Error reading properties");
		}
		return 0;
	}

    
}
