package db2;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.print.DocFlavor.STRING;

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
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		new Table(strTableName, strClusteringKeyColumn, htblColNameType);
	}

	// following method creates a B+tree index
	// CreateIndex Should be done ,just need Testing
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException, IOException {
		ArrayList<String[]> metaData = Tool.readMetaData(strTableName);
		if (!Tool.isTableUnique(strTableName)) {
			if (!Tool.checker(metaData, strColName)) {
				throw new DBAppException("Column " + strColName + "  name does not exist");
			}

			else {
				Table t = Tool.deserializeTable(strTableName);
				if (t.Indices.containsKey(strColName)) {
					throw new DBAppException("Column " + strColName + " index already created.");
				}

				t.addIndex(strColName, new bplustree(Tool.readBtreeOrder("config/DBApp.properties")));
				for (int i = 0; i < t.getPageCount(); i++) {
					Page p = Tool.deserializePage(t, i);
					int j = 0;
					for (Tuple tuple : p.getTuples()) {
						int key = tuple.getValue(strColName).hashCode();
						j++;
						Double encoder = Tool.encoder(i, j);
						// Important I dont add the original unique value to duplicate onloy keep record
						// of its duplicates
						// So when Updating/Deleting I should check first if there is duplicate and
						// delete/update duplicate instead of original
						// value in my B+Tree
						if (t.getIndices().get(strColName).search(key) != null) {
							// Check Duplicate Again
							if (!t.duplicates.containsKey(strColName)) {
								// If not, create a new inner hashtable for the key
								t.duplicates.put(strColName, new Hashtable<Integer, Vector<Double>>());
							}
							Hashtable<Integer, Vector<Double>> innerHashtable = t.duplicates.get(strColName);
							// Check if the inner hashtable already contains the key
							if (!innerHashtable.containsKey(key)) {
								// If not, create a new vector for the key
								innerHashtable.put(key, new Vector<Double>());
							}
							// Get the vector associated with the key
							Vector<Double> vector = innerHashtable.get(key);
							vector.add(encoder);
						} else {
							t.getIndices().get(strColName).insert(key, encoder);
						}
					}
				}
				Tool.updateMetaData(strTableName, strColName, strIndexName);
			}
		} else {
			throw new DBAppException("Table is not Found in Meta Data");
		}
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	// Insert Should be Done,Testing Left
	// Check if Object is one of my 3 types corresponding to strColname
	public static void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			Table table = Tool.deserializeTable(strTableName);
			// Clusterkey exists and check for clustering duplicates
			Set<String> tableColumnNames = Tool.getColumNameFromMetaData(strTableName);
			for (String columnName : htblColNameValue.keySet()) {
				if (!tableColumnNames.contains(columnName) && tableColumnNames.size() != htblColNameValue.size()) {
					throw new DBAppException(
							"Column '" + columnName + "' does not exist in table or not equal size'" + strTableName
									+ "'.");
				}
			}
			if (!htblColNameValue.containsKey(table.getClusterKey())) {
				throw new DBAppException("Clustering key '" + table.getClusterKey() + "' value is missing.");
			}
			// Update each Btree that exists
			// With Duplicate Case
			for (String strColumnName : htblColNameValue.keySet()) {
				if (table.getIndices().containsKey(strColumnName)) {
					int key = htblColNameValue.get(strColumnName).hashCode();
					Page page = Tool.deserializePage(table, table.getPageCount());
					double encoder = Tool.encoder(table.getPageCount(), (page.gettupleCount() + 1));// gettupleCount()returns
																									// last inserted
																									// tuple tupleid is
																									// the next one
					if (table.getIndices().get(strColumnName).search(key) != null) {
						// Check Duplicate Again
						if (!table.duplicates.containsKey(strColumnName)) {
							// If not, create a new inner hashtable for the key
							table.duplicates.put(strColumnName, new Hashtable<Integer, Vector<Double>>());
						}
						Hashtable<Integer, Vector<Double>> innerHashtable = table.duplicates.get(strColumnName);
						// Check if the inner hashtable already contains the key
						if (!innerHashtable.containsKey(key)) {
							// If not, create a new vector for the key
							innerHashtable.put(key, new Vector<Double>());
						}
						// Get the vector associated with the key
						Vector<Double> vector = innerHashtable.get(key);
						vector.add(encoder);
					} else {
						table.getIndices().get(strColumnName).insert(key, encoder);
					}
				}
			}
			Page p = Tool.deserializePage(table, table.getPageCount());
			int pageID, tupleID;
			if (p.isFull()) {
				pageID = table.getPageCount() + 1;
				tupleID = 1;
			} else {
				pageID = table.getPageCount();
				tupleID = p.gettupleCount() + 1;
			}
			double encodedID = Tool.encoder(pageID, tupleID);
			// Create my new Tuple
			Tuple tuple = new Tuple(htblColNameValue, encodedID);
			// Insert int table's pages
			table.insertTupleIntoLastPage(tuple);
			// Save Table
			Tool.serializeTable(table);
		} catch (Exception e) {
			throw new DBAppException("Error inserting into table: " + e.getMessage());
		}
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, // suspicious //Update is Wrong needs Rework
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			Table table = Tool.deserializeTable(strTableName);
			int key = strClusteringKeyValue.hashCode();
			Double temp = table.getIndices().get(table.clusterKey).search(key);
			if (temp != null) {
				String encoder = temp + "";
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
			} else {
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
	// htblColNameValue enteries are ANDED together ??
	// Delete Pages not coressponding to pagecount,make new vector
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException { // rework
		try {

			Table table = Tool.deserializeTable(strTableName);

			if (table == null) {
				throw new DBAppException("Table '" + strTableName + "' does not exist.");
			}

			Set<String> tableColumnNames = Tool.getColumNameFromMetaData(strTableName);
			for (String columnName : htblColNameValue.keySet()) {
				if (!tableColumnNames.contains(columnName) && tableColumnNames.size() != htblColNameValue.size()) {
					throw new DBAppException(
							"Column '" + columnName + "' does not exist in table or not equal size'" + strTableName
									+ "'.");
				}
			}

			if (!htblColNameValue.containsKey(table.getClusterKey())) {
				throw new DBAppException("Clustering key '" + table.getClusterKey() + "' value is missing.");
			}

			if (htblColNameValue.isEmpty()) {
				Table table2 = Tool.deserializeTable(strTableName);
				for (int i = 0; i < table2.getPageCount(); i++) {
					table.deletePage(i);
				}
				Tool.serializeTable(table2);
			} else {
				SQLTerm[] sqlTerm = new SQLTerm[htblColNameValue.size()];
				int i = 0;

				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String columnName = entry.getKey();
					Object columnValue = entry.getValue();
					sqlTerm[i++] = new SQLTerm(strTableName, columnName, "=", columnValue);
				}
				String[] andSTR = { "AND" };
				ArrayList<Tuple> toBeDeleted = new ArrayList<>();

				Iterator<Tuple> iterator = selectFromTable(sqlTerm, andSTR);
				while (iterator.hasNext()) {
					Tuple tuple = iterator.next();
					toBeDeleted.add(tuple);
				}

				for (int pageId = 1; pageId <= table.getPageCount(); pageId++) {
					Page page = Tool.deserializePage(table, pageId);

					for (int tupleId = 1; tupleId <= page.getTuples().size(); tupleId++) {
						Tuple tuple = page.getTuple(tupleId);

						for (int k = 0; k < toBeDeleted.size(); k++) {
							if (((Tuple) toBeDeleted.get(k)).getTupleID() == tuple.getTupleID()) {
								page.deleteTuple(tupleId);
							}
						}

					}
				}
			}

			Tool.serializeTable(table);
		} catch (IOException e) {
			throw new DBAppException("An error occurred while deleting from table: " + e.getMessage());
		}
	}

	// USE BTREE SEARCH RANGES for SERACH(MIN,MAX)
	// THEN SORT ARRAYLIST TO DESERIALIZE ONE TIME
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		ArrayList<Tuple> filteredTuples = new ArrayList<>();

		for (int i = 0; i < arrSQLTerms.length; i++) {
			String tableName = arrSQLTerms[i]._strTableName;
			String columnName = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Object value = arrSQLTerms[i]._objValue;
			Table table = Tool.deserializeTable(tableName);
			ArrayList<Tuple> currentFilteredTuples = Tool.filterTuplesByOperator(table, columnName, operator, value);
			if (i == 0) {
				filteredTuples.addAll(currentFilteredTuples);
			} else {
				String logicalOperator = strarrOperators[i - 1];
				switch (logicalOperator) {
					case "AND":
						filteredTuples.retainAll(currentFilteredTuples);
						break;
					case "OR":
						filteredTuples.addAll(currentFilteredTuples);
						break;
					case "XOR":
						List<Tuple> temp = new ArrayList<>(filteredTuples);
						temp.removeAll(currentFilteredTuples);
						currentFilteredTuples.removeAll(filteredTuples);
						filteredTuples.addAll(temp);
						filteredTuples.addAll(currentFilteredTuples);
						break;
					default:

						break;
				}
			}
		}
		return filteredTuples.iterator();
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