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

    public static void main( String[] args ){
        System.err.println(Tool.readPageSize("config//DBApp.properties"));
        String strTableName = "Student";
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        
        try {
            createTable(strTableName, "id", htblColNameType);
        } catch (DBAppException e) {
            System.err.println(e.getMessage());
        }
    }
    public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		if (Tool.isTableUnique(strTableName)) {
			Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, null);
		}

	}
}
