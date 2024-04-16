package db2;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Hashtable;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }
    public static void main( String[] args ) throws DBAppException{
        // System.err.println(Tool.readPageSize("config//DBApp.properties"));
        // String strTableName = "Student1";
        // Hashtable htblColNameType = new Hashtable( );
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
        // File dataDir = new File("Tables");
		// Table t1=new Table(strTableName, "id", htblColNameType);
        // t1.CreateNewPage();
        //  strTableName = "Student2";
        // htblColNameType = new Hashtable( );
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
        //  dataDir = new File("Tables");
		// Table t2=new Table(strTableName, "id", htblColNameType);
        // t2.CreateNewPage();
        // t2.CreateNewPage();
        // Tool.updateMetaData("strTableName", "gpa","gpaIndex" );

        Hashtable<String, Object> sampleData1 = new Hashtable<>();
        sampleData1.put("ID", 1);
        sampleData1.put("Name", "John");
        sampleData1.put("Age", 30);

        Hashtable<String, Object> sampleData2 = new Hashtable<>();
        sampleData2.put("ID", 2);
        sampleData2.put("Name", "Alice");
        sampleData2.put("Age", 25);

        // Create tuples
        Tuple tuple1 = new Tuple(sampleData1, "ID");
        Tuple tuple2 = new Tuple(sampleData2, "ID");

        // Test toString method
        System.out.println("Tuple 1: " + tuple1.toString());
        System.out.println("Tuple 2: " + tuple2.toString());
        
    }
    
}
