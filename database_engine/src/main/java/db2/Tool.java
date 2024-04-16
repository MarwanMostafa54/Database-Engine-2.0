package db2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
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
            String path = "./Tables/" + T.getTableName() + "_Properties" + ".ser";
            path = path.replaceAll("[^a-zA-Z0-9()_./+]", "");
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
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            // Create the data directory if it doesn't exist
            if (!dataDir.mkdirs()) {
                System.err.println("Failed to create data directory.");
                return;
            }
        }

        String filePath = "data/metadata.csv"; // Use the same directory structure
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

    public static boolean isTableUnique(String strTableName) throws DBAppException {
        try (BufferedReader reader = new BufferedReader(new FileReader("data//metadata.csv"))) {
            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
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
    
}
