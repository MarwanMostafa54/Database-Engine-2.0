package db2;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Hashtable;

public class DBApp {

	public DBApp() {

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public static void init() {

		Tool.initializeMetaData();
		Tool.initializeProperties();
		File dataDir = new File("Tables");
		if (!dataDir.exists()) {
			// Create the data directory if it doesn't exist
			if (!dataDir.mkdirs()) {
				System.err.println("Failed to create data directory.");
				return;
			}
		}
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public  void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
			Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
		String strColName,
		String strIndexName) throws DBAppException, IOException {
		ArrayList<String[]> metaData = Tool.readMetaData(strTableName);
		if (!Tool.isTableUnique(strTableName)) {
				if(!Tool.checker(metaData,strColName)){
					throw new DBAppException("Column " + strColName + "  name does not exist");}
				
				else{
				Table t= Tool.deserializeTable(strTableName);
				if (t.columns.containsKey(strColName)) {
					throw new DBAppException("Column " + strColName + " index already created.");}

				t.addColumn(strColName, new bplustree(Tool.readBtreeOrder("config/DBApp.properties")));	
				for (int i = 0; i < t.getPageCount(); i++) {	
					Page p= Tool.deserializePage(t,i);
					int j=0;
					for (Tuple tuple : p.getTuples()) {
						int key = tuple.getValue(strColName).hashCode();
						j++;
						String temp=i+"."+j;
						BigDecimal number = new BigDecimal(temp);
						Double encoder=number.doubleValue();
						if(t.getColumns().get(strColName).search(key)!=null){

						}
						else{
						t.getColumns().get(strColName).insert(key, encoder);}
					}
				}
				Tool.updateMetaData(strTableName, strColName,strIndexName);
			}
		}
		else {
			throw new DBAppException("Table is not Found in Meta Data");
		}
	}
	

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, //suspicious
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
				try{
					Table table = Tool.deserializeTable(strTableName);
					int key = strClusteringKeyValue.hashCode();
					Double temp = table.getColumns().get(table.clusterKey).search(key);
					if(temp != null){
						String encoder = temp + "" ;
						String[] data = encoder.split(".");
						int pageID = Integer.parseInt(data[0]);
						int tupleID = Integer.parseInt(data[1]);
						Page p = Tool.deserializePage(table, pageID);
						Tuple t = p.getTuple(tupleID);
						// Update the specified columns with the new values
						for (String columnName : htblColNameValue.keySet()) {
							t.setValue(columnName, htblColNameValue.get(columnName).toString());
						}
						Tool.serializePage(table, p);
					}
					 else {
						throw new DBAppException("Tuple with clustering key value '" + strClusteringKeyValue + "' not found.");
					}
				}
				
				catch (Exception e) {
					throw new DBAppException("Error updating table: " + e.getMessage());
				}
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException {

		return null;
	}

	public static void main(String[] args) {
		init();

		// try{
		// String strTableName = "Student";
		// DBApp dbApp = new DBApp( );

		// Hashtable htblColNameType = new Hashtable( );
		// htblColNameType.put("id", "java.lang.Integer");
		// htblColNameType.put("name", "java.lang.String");
		// htblColNameType.put("gpa", "java.lang.double");
		// dbApp.createTable( strTableName, "id", htblColNameType );
		// dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

		// Hashtable htblColNameValue = new Hashtable( );
		// htblColNameValue.put("id", new Integer( 2343432 ));
		// htblColNameValue.put("name", new String("Ahmed Noor" ) );
		// htblColNameValue.put("gpa", new Double( 0.95 ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 453455 ));
		// htblColNameValue.put("name", new String("Ahmed Noor" ) );
		// htblColNameValue.put("gpa", new Double( 0.95 ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 5674567 ));
		// htblColNameValue.put("name", new String("Dalia Noor" ) );
		// htblColNameValue.put("gpa", new Double( 1.25 ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 23498 ));
		// htblColNameValue.put("name", new String("John Noor" ) );
		// htblColNameValue.put("gpa", new Double( 1.5 ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// htblColNameValue.clear( );
		// htblColNameValue.put("id", new Integer( 78452 ));
		// htblColNameValue.put("name", new String("Zaky Noor" ) );
		// htblColNameValue.put("gpa", new Double( 0.88 ) );
		// dbApp.insertIntoTable( strTableName , htblColNameValue );

		// SQLTerm[] arrSQLTerms;
		// arrSQLTerms = new SQLTerm[2];
		// arrSQLTerms[0]._strTableName = "Student";
		// arrSQLTerms[0]._strColumnName= "name";
		// arrSQLTerms[0]._strOperator = "=";
		// arrSQLTerms[0]._objValue = "John Noor";

		// arrSQLTerms[1]._strTableName = "Student";
		// arrSQLTerms[1]._strColumnName= "gpa";
		// arrSQLTerms[1]._strOperator = "=";
		// arrSQLTerms[1]._objValue = new Double( 1.5 );

		// String[]strarrOperators = new String[1];
		// strarrOperators[0] = "OR";
		// // select * from Student where name = "John Noor" or gpa = 1.5;
		// Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		// }
		// catch(Exception exp){
		// exp.printStackTrace( );
		// }
	}

}