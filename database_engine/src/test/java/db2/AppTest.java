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
        String strTableName = "Student1";
        Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
        File dataDir = new File("Tables");
		Table t1=new Table(strTableName, "id", htblColNameType);
        t1.CreateNewPage();
         strTableName = "Student2";
        htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
         dataDir = new File("Tables");
		Table t2=new Table(strTableName, "id", htblColNameType);
        t2.CreateNewPage();
        t2.CreateNewPage();
        t2.deletePage(t2.CreateNewPage());

    }
    
}
