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
        System.err.println(Tool.readPageSize("config//DBApp.properties"));
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
        File dataDir = new File("Tables");
		Table t=new Table(strTableName, "id", htblColNameType, "Tables");
    }
    
}
