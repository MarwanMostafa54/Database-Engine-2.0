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

import java.util.ArrayList;
import java.util.Hashtable;

//
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
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		this.createIndex(strTableName, table.getClusterKey(), "Clustering Key");
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
				if (t.Indices.containsKey(strColName) && (strColName.equals(t.getClusterKey()))) {
					throw new DBAppException("Column " + strColName + " is the Clustering Key,Already sorted.");
				}
				if (t.Indices.containsKey(strColName)) {
					throw new DBAppException("Column " + strColName + " index already created.");
				}

				t.addIndex(strColName, new bplustree(Tool.readBtreeOrder("config/DBApp.properties")));
				for (int i = 1; i <= t.getPageCount(); i++) {
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
				Tool.serializeTable(t);
			}
		} else {
			throw new DBAppException("Table is not Found in Meta Data");
		}
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	// Insert Should be Done,Testing Left
	// Check if Object is one of my 3 types corresponding to strColname
	// Contains Duplicated Values
	public static void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			Table table = Tool.deserializeTable(strTableName);
			// Clusterkey exists and check for clustering duplicates
			if (!Tool.CheckType(htblColNameValue, table)) {
				throw new DBAppException("Wrong Type corresponding to its Column");
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
			} else {
				Double searchResult = table.getIndices().get(table.getClusterKey())
						.search(htblColNameValue.get(table.getClusterKey()).hashCode());
				if (searchResult != null)// Duplicate ClusteringKeyValue entered
				{
					throw new DBAppException("Clustering key '" + table.getClusterKey() + "' value is not Unique.");
				}
			}
			// Update each Btree that exists
			// With Duplicate Case

			int pageID, tupleID;
			if (table.getPageCount() > 0) {
				Page p = Tool.deserializePage(table, table.getPageCount());
				if (p.isFull()) {
					pageID = table.getPageCount() + 1;
					tupleID = 1;
				} else {
					pageID = table.getPageCount();
					tupleID = p.gettupleCount() + 1;
				}
				Tool.serializePage(table, p);
			} else {
				pageID = table.getPageCount() + 1;
				tupleID = 1;
			}

			double encodedID = Tool.encoder(pageID, tupleID);
			// Create my new Tuple
			Tuple tuple = new Tuple(htblColNameValue, encodedID);
			// Insert int table's pages
			table.insertTupleIntoLastPage(tuple);
			// Save Table

			for (String strColumnName : htblColNameValue.keySet()) {
				if (table.getIndices().containsKey(strColumnName) && table.getPageCount() > 0) {
					int key = htblColNameValue.get(strColumnName).hashCode();
					Page page = Tool.deserializePage(table, table.getPageCount());
					System.out.println(table.getPageCount());
					double encoder = Tool.encoder(table.getPageCount(), page.gettupleCount());// gettupleCount()returns
																								// last inserted
																								// tuple tupleid is
																								// the next one
					System.out.println("PageId" + table.getPageCount());
					System.out.println(("TupleId" + page.gettupleCount()));
					System.out.println("Encoded" + encoder);
					Double searchResult = table.getIndices().get(strColumnName).search(key);
					if (searchResult != null) {
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
			Tool.serializeTable(table);
		} catch (Exception e) {
			throw new DBAppException("Error inserting into table: " + e.getMessage());
		}
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	// Problem with UpdateTable SSOLVED WITH ORDER ALSO ONLY LEFT TO TEST BTREE
	// INSERT PLUS UPDATE BTREES
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		ArrayList<String[]> metaData = Tool.readMetaData(strTableName);
		if (strClusteringKeyValue.isEmpty()) {
			throw new DBAppException("Empty ClusteringKeyValue Entered");
		}
		if (metaData.isEmpty()) {
			throw new DBAppException(strTableName + " table does not exist");
		}

		Object identity = strClusteringKeyValue;
		for (String[] data : metaData) {
			// System.out.println(data[3]);
			if (data[3].equals("true")) {
				// System.out.println("In");
				if (data[2].equalsIgnoreCase("java.lang.double")) {
					identity = Double.parseDouble(strClusteringKeyValue);
					// System.out.println(data[2]);
					break;
				}
				if (data[2].equalsIgnoreCase("java.lang.integer")) {
					identity = Integer.parseInt(strClusteringKeyValue);
					// System.out.println(data[2]);
					break;
				}

			}
		}

		try {
			Tuple old;
			Hashtable<String, String> OldEntries = new Hashtable<>();
			Table table = Tool.deserializeTable(strTableName);
			Hashtable<String, Object> temp = new Hashtable<>();
			temp.put(table.getClusterKey(), identity);
			if (!Tool.CheckType(htblColNameValue, table)) {
				throw new DBAppException("Wrong Type corresponding to its Column");
			}
			if (!Tool.CheckType(temp, table)) {
				throw new DBAppException("Wrong Type corresponding to Clustering Key Column");
			}

			if (htblColNameValue.containsKey(table.getClusterKey())) {
				throw new DBAppException("Clustering key included in my htblColNameValue");
			}
			int key = identity.hashCode();

			bplustree tree = table.getIndices().get(table.getClusterKey());
			System.out.println("done");
			if (tree.search(key) == null || tree.search(key) == 0) {
				throw new DBAppException("Clustering Key Value Does Not Exist");
			}
			
			if (table.duplicates.containsKey(table.getClusterKey())){
				System.out.println("enters");
				if( table.duplicates.get(table.getClusterKey()).containsKey(key)) {
				// Duplicates for this list exist
				Vector<Double> Values = table.duplicates.get(table.getClusterKey()).get(key);
				Double value = Values.get(Values.size() - 1);
				ArrayList<Integer> decode = Tool.decoder(value);
				Page page = Tool.deserializePage(table, decode.get(0));
				old = page.getTuple(decode.get(1));
				OldEntries = old.getTupleInfo();
				page.getTuple(decode.get(1)).updateTuple(htblColNameValue, table.getClusterKey());
				Tool.serializePage(table, page);
			} 
			}
			else {
				// No duplicates, change one on Search
				System.out.println("enters");
				Double value = tree.search(key);
				System.out.println(value);
				ArrayList<Integer> decode = Tool.decoder(value);
				System.out.println(decode.get(0));
				System.out.println(decode.get(1));
				Page page = Tool.deserializePage(table, decode.get(0));

				old = page.getTuple(decode.get(1));
				System.out.println(old.toString());
				if(old.getHashtable()==null){
					System.err.println("error");
				}
				if(old.getTupleInfo()==null){
					System.err.println("error");
				}
				OldEntries = old.getTupleInfo();
				System.out.println("continue");
				old.updateTuple(htblColNameValue, table.getClusterKey());
				System.out.println(old.toString());
				Tool.serializePage(table, page);
			}
			
			if (!OldEntries.isEmpty()) { // Update B-trees From Old To New
				System.out.println("continue");
				Tool.UpdateBtrees(table, OldEntries, htblColNameValue, key,metaData);
			}
			Tool.serializeTable(table);
		} catch (Exception e) {
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

	// Make Note of Order of Hashtable
	// Create Index Btree Not Serializable with its subclasses
	public static void main(String[] args) {
		init();

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

			// Hashtable htblColNameType = new Hashtable();
			// htblColNameType.put("id", "java.lang.Integer");
			// htblColNameType.put("name", "java.lang.String");
			// htblColNameType.put("gpa", "java.lang.double");
			// dbApp.createTable(strTableName, "id", htblColNameType);
			// dbApp.createIndex(strTableName, "gpa", "GpaIndex");

			 Hashtable htblColNameValue = new Hashtable();
			// htblColNameValue.put("id", new Integer(2343432));
			// htblColNameValue.put("name", new String("Ahmed Noor"));
			// htblColNameValue.put("gpa", new Double(0.95));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(453455));
			// htblColNameValue.put("name", new String("Ahmed Noor"));
			// htblColNameValue.put("gpa", new Double(0.95));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5674567));
			// htblColNameValue.put("name", new String("Dalia Noor"));
			// htblColNameValue.put("gpa", new Double(1.25));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(555));
			// htblColNameValue.put("name", new String("New"));
			// htblColNameValue.put("gpa", new Double(1.5));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(123));
			// htblColNameValue.put("name", new String("MO"));
			// htblColNameValue.put("gpa", new Double(2.5));
			// dbApp.insertIntoTable(strTableName, htblColNameValue);

			// System.out.println(Tool.encoder(1,0));
			// System.out.println(Tool.decoder(Tool.encoder(1,2)));

			htblColNameValue.clear();
			htblColNameValue.put("name", new String("MEEEE "));
			htblColNameValue.put("gpa", new Double(0.88));
			dbApp.updateTable(strTableName, "123", htblColNameValue);

			Table table = Tool.deserializeTable(strTableName);
			// System.out.println(table.getPageCount());
			for (int i = 1; i < 2; i++) {
			Page page = Tool.deserializePage(table, i);
			System.out.println(page.toString());
			}

			// htblColNameValue.clear();
			// htblColNameValue.clear();
			// htblColNameValue.put("name", new String("New Version"));
			// htblColNameValue.put("gpa", new Double(0.93));
			// dbApp.updateTable(strTableName, "5674567", htblColNameValue);

			// table = Tool.deserializeTable(strTableName);
			// for (int i = 1; i < 6; i++) {
			// Page page = Tool.deserializePage(table, i);
			// System.out.println(page.toString());
			// }

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
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}
}
