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
import java.util.Arrays;
import java.util.HashSet;
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
					if (tuple != null) {
						int key = tuple.getValue(strColName).hashCode();
						j++;
						Double encoder = Tool.encoder(i, j);
						// Important I dont add the original unique value to duplicate onloy keep record
						// of its duplicates
						// So when Updating/Deleting I should check first if there is duplicate and
						// delete/update duplicate instead of original
						// value in my B+Tree
						if (t.getIndices().get(strColName).search(key) != null) {
							t.getIndices().get(strColName).insert(key, encoder);
						}
					}
				}
			}
			Tool.updateMetaData(strTableName, strColName, strIndexName);
			Tool.serializeTable(t);

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
				System.out.println("Ok");
				bplustree tree = table.getIndices().get(table.getClusterKey());

				// Check if the tree object is not null
				if (tree != null) {
					// Get the clustering key value from htblColNameValue
					Object clusteringKeyValue = htblColNameValue.get(table.getClusterKey());

					// Check if the clustering key value is not null
					if (clusteringKeyValue != null) {
						// Compute the hash code of the clustering key value
						int hashCode = clusteringKeyValue.hashCode();

						// Invoke the search method on the bplustree object
						Double searchResult = tree.search(hashCode);
						System.out.println("Ok");
						if (searchResult != null)// Duplicate ClusteringKeyValue entered
						{
							throw new DBAppException(
									"Clustering key '" + table.getClusterKey() + "' value is not Unique.");
						}
					}
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
					int key = tuple.getColumnValue(strColumnName).hashCode();
					Page page = Tool.deserializePage(table, table.getPageCount());
					// System.out.println(table.getPageCount());
					double encoder = Tool.encoder(page.getPageID(), page.gettupleCount());// gettupleCount()returns
																							// last inserted
																							// tuple tupleid is
																							// the next one
					System.out.println("PageId" + table.getPageCount());
					System.out.println(("TupleId" + page.gettupleCount()));
					System.out.println("Encoded" + encoder);

					table.getIndices().get(strColumnName).insert(key, encoder);
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
			htblColNameValue.put(table.getClusterKey(), strClusteringKeyValue);
			Tuple tuple = new Tuple(htblColNameValue, 0);
			System.out.println("1");
			int key = tuple.getColumnValue(table.getClusterKey()).hashCode();
			System.out.println("2");
			bplustree tree = table.getIndices().get(table.getClusterKey());
			System.out.println("done");
			if (tree.search(key, key) == null || tree.search(key) == null) {
				throw new DBAppException("Clustering Key Value Does Not Exist");
			}
			ArrayList<Double> values = tree.search(key, key);
			double value = values.get(0);
			ArrayList<Integer> decode = Tool.decoder(value);
			// System.out.println(decode.get(0));
			// System.out.println(decode.get(1));
			Page page = Tool.deserializePage(table, decode.get(0));
			old = page.getTuple(decode.get(1));
			System.out.println(old.toString());

			old.updateTuple(htblColNameValue, table.getClusterKey());
			System.out.println(old.toString());
			Tool.serializePage(table, page);
			System.out.println("ok");
			for (String Key : table.getIndices().keySet()) {
				if (!(Key.equals(table.getClusterKey()))) {
					int key1 = tuple.getColumnValue(Key).hashCode();
					// double temp = tree.search(key1);
					// tree.delete(key1);
					if (table.getIndices().get(Key).search(key1) != null) {
						// Check Duplicate Again
						double x = table.getIndices().get(Key).search(key1);
						table.getIndices().get(Key).delete(key1);
						int key2 = old.getColumnValue(Key).hashCode();
						table.getIndices().get(Key).insert(key2, x);
					}
				}
			}
			Tool.serializeTable(table);
		} catch (Exception e) {
			throw new DBAppException("Error updating table: " + e.getMessage());
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			Table table = Tool.deserializeTable(strTableName);

			if (table == null) {
				throw new DBAppException("Table '" + strTableName + "' does not exist.");
			}

			Set<String> tableColumnNames = Tool.getColumNameFromMetaData(strTableName);
			for (String columnName : htblColNameValue.keySet()) {
				if (!tableColumnNames.contains(columnName)) {
					throw new DBAppException(
							"Column '" + columnName + "' does not exist in table '" + strTableName + "'.");
				}
			}

			if (htblColNameValue.isEmpty()) {
				int temp = table.getPageCount();
				for (int pageId = 1; pageId <= temp; pageId++) {
					table.deletePage(pageId);
				}
			} else {
				// Delete tuples matching the given conditions
				SQLTerm[] sqlTerms = new SQLTerm[htblColNameValue.size()];
				int i = 0;
				for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
					String columnName = entry.getKey();
					Object columnValue = entry.getValue();
					sqlTerms[i++] = new SQLTerm(strTableName, columnName, "=", columnValue);
				}
				String[] andSTR = new String[htblColNameValue.size() - 1];
				Arrays.fill(andSTR, "AND");
				ArrayList<Tuple> toBeDeleted = new ArrayList<>();
				Iterator<Tuple> iterator = selectFromTable(sqlTerms, andSTR);
				while (iterator.hasNext()) {
					toBeDeleted.add(iterator.next());
				}
				// Delete tuples from pages
				for (Tuple tuple : toBeDeleted) {
					ArrayList<Integer> Location = Tool.decoder(tuple.getTupleID());
					table.deleteTuple(Location.get(0), Location.get(1));
					// updatebtree locations
				}
			}
			Hashtable<String, bplustree> indices = table.getIndices();
			Set<String> columnNames = indices.keySet();
			if (htblColNameValue.isEmpty()) {
				indices.clear();
			} else {
				indices.clear();
				for (String col : columnNames) {
					createIndex(strTableName, col, Tool.getIndexName(col));
				}
			}

			Tool.serializeTable(table);
		} catch (IOException e) {
			throw new DBAppException("An error occurred while deleting from table: " + e.getMessage());
		}
	}

	// USE BTREE SEARCH RANGES for SERACH(MIN,MAX)
	// THEN SORT ARRAYLIST TO DESERIALIZE ONE TIME
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException, IOException {
		ArrayList<Tuple> filteredTuples = new ArrayList<>();
		ArrayList<Tuple> currentFilteredTuples = new ArrayList<>();

		for (int i = 0; i < arrSQLTerms.length; i++) {
			String tableName = arrSQLTerms[i]._strTableName;
			String columnName = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Object value = arrSQLTerms[i]._objValue;
			Table table = Tool.deserializeTable(tableName);

			Set<String> indexedColumns = table.getIndices().keySet();

			boolean hasIndex = indexedColumns.contains(columnName);

			if (hasIndex) {
				// If index exists, use B+ tree search
				bplustree tree = table.getIndices().get(columnName);
				ArrayList<Double> pageCodes = new ArrayList<>();
				if (operator.equals("=")) {
					pageCodes = tree.search(value.toString().hashCode(), value.toString().hashCode());
				} else if (operator.equals(">=")) {
					pageCodes = tree.search(value.toString().hashCode(), Integer.MAX_VALUE);
				} else if (operator.equals(">")) {
					pageCodes = tree.search(value.toString().hashCode() + 1, Integer.MAX_VALUE);
				} else if (operator.equals("<")) {
					pageCodes = tree.search(Integer.MIN_VALUE, value.toString().hashCode() - 1);
				} else if (operator.equals("<=")) {
					pageCodes = tree.search(Integer.MIN_VALUE, value.toString().hashCode());
				} else if (operator.equals("!=")) {
					pageCodes = tree.search(Integer.MIN_VALUE, value.toString().hashCode() - 1);
					pageCodes.addAll(tree.search(value.toString().hashCode() + 1, Integer.MAX_VALUE));
				}

				for (double code : pageCodes) {
					ArrayList<Integer> temp = Tool.decoder(code);
					Page p = Tool.deserializePage(table, temp.get(0));
					Tuple tuple = p.getTuple(temp.get(1));
					currentFilteredTuples.add(tuple);
					Tool.serializePage(table, p);
				}

			} else {
				// If index does not exist, filter tuples using operator
				currentFilteredTuples = Tool.filterTuplesByOperator(table, columnName, operator, value);
			}

			if (i == 0) {
				filteredTuples.addAll(currentFilteredTuples);
			} else {
				String logicalOperator = strarrOperators[i - 1];
				switch (logicalOperator) {
					case "AND":
						ArrayList<Tuple> temp1 = new ArrayList<Tuple>();
						for (Tuple tuple : filteredTuples) {
							for (Tuple tuple1 : currentFilteredTuples) {
								if (tuple1.toString().equals(tuple.toString())) {
									temp1.add(tuple);

								}
							}
						}
						filteredTuples = temp1;
						// filteredTuples.retainAll(currentFilteredTuples);
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
		ArrayList<Tuple> temp = new ArrayList<Tuple>();
		boolean flag = false;
		for (Tuple tuble : filteredTuples) {
			for (Tuple tuble1 : temp) {
				if (tuble1.toString().equals(tuble.toString())) {
					flag = true;
					break;
				}
			}
			if (!flag) {
				temp.add(tuble);
			} else {
				flag = false;
			}
		}
		filteredTuples = temp;
		return filteredTuples.iterator();
	}

	// Make Note of Order of Hashtable
	// Create Index Btree Not Serializable with its subclasses
	public static void main(String[] args) {
		init();

		try {
			// String strTableName = "Student";
			// DBApp dbApp = new DBApp();

			// Hashtable htblColNameType = new Hashtable();
			// // htblColNameType.put("id", "java.lang.Integer");
			// // htblColNameType.put("name", "java.lang.String");
			// // htblColNameType.put("gpa", "java.lang.double");
			// // dbApp.createTable(strTableName, "id", htblColNameType);
			// // dbApp.createIndex(strTableName, "gpa", "GpaIndex");

			// Hashtable htblColNameValue = new Hashtable();
			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(55));
			// // htblColNameValue.put("name", new String("Marwan"));
			// // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(56));
			// // htblColNameValue.put("name", new String("Ahmed Noor"));
			// // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(57));
			// // htblColNameValue.put("name", new String("Dalia Noor"));
			// // htblColNameValue.put("gpa", new Double(1.25));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(58));
			// // htblColNameValue.put("name", new String("New"));
			// // htblColNameValue.put("gpa", new Double(1.5));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(59));
			// // htblColNameValue.put("name", new String("MO"));
			// // htblColNameValue.put("gpa", new Double(2.5));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(1));
			// // htblColNameValue.put("name", new String("Marwan"));
			// // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(2));
			// // htblColNameValue.put("name", new String("Ahmed Noor"));
			// // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(3));
			// // htblColNameValue.put("name", new String("Dalia Noor"));
			// // htblColNameValue.put("gpa", new Double(1.25));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(4));
			// // htblColNameValue.put("name", new String("New"));
			// // htblColNameValue.put("gpa", new Double(1.5));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(5));
			// // htblColNameValue.put("name", new String("MO"));
			// // htblColNameValue.put("gpa", new Double(2.5));
			// // dbApp.insertIntoTable(strTableName, htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("name", new String("Nofal "));
			// // htblColNameValue.put("gpa", new Double(2.7));
			// // dbApp.updateTable(strTableName, "55", htblColNameValue);

			// // htblColNameValue.clear();
			// // htblColNameValue.put("name", new String("good boy "));
			// // htblColNameValue.put("gpa", new Double(1.7));
			// // dbApp.updateTable(strTableName, "1", htblColNameValue);

			// // System.out.println(Tool.encoder(1, 0));
			// // System.out.println(Tool.decoder(Tool.encoder(1, 2)));

			// htblColNameValue.clear();
			// // htblColNameValue.put("gpa", new Double(1.25));
			// // htblColNameValue.put("name", new String("New"));
			// dbApp.deleteFromTable(strTableName, htblColNameValue);

			// Table table = Tool.deserializeTable(strTableName);
			// // table.CreateNewPage();
			// System.out.println(table.getPageCount());
			// for (int i = 1; i <= table.getPageCount(); i++) {
			// Page page = Tool.deserializePage(table, i);
			// System.out.println(page.toString());
			// }

			// // table = Tool.deserializeTable(strTableName);
			// // // table.CreateNewPage();
			// // System.out.println(table.getPageCount());
			// // for (int i = 1; i < 2; i++) {
			// // Page page = Tool.deserializePage(table, i);
			// // System.out.println(page.toString());
			// // }

			// // // Table table = Tool.deserializeTable(strTableName);
			// // for (int i = 1; i < 3; i++) {
			// // Page page = Tool.deserializePage(table, i);
			// // System.out.println(page.toString());
			// // }

			// // if (table.getIndices().containsKey("gpa")) {
			// // System.out.println("nice");
			// // }
			// // Object x = 0.95;
			// // int key = x.hashCode();
			// // System.out.println(table.getIndices().get("gpa").search(key));

			// // htblColNameValue.clear();
			// // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			// // Table table = Tool.deserializeTable(strTableName);
			// // if(table.getIndices().containsKey("gpa")){
			// // System.out.println("nice");
			// // }
			// // Object x=0.95;
			// // int key=x.hashCode();
			// // System.out.println(table.getIndices().get("gpa").search(key));
			// // System.out.println(table.getPageCount());
			// // for (int i = 1; i <= table.pageCount; i++) {
			// // Page page = Tool.deserializePage(table, i);
			// // System.out.println(page.toString());
			// // }

			// // htblColNameValue.clear();
			// // htblColNameValue.clear();
			// // htblColNameValue.put("name", new String("New Version"));
			// // htblColNameValue.put("gpa", new Double(0.93));
			// // dbApp.updateTable(strTableName, "5674567", htblColNameValue);

			// // table = Tool.deserializeTable(strTableName);
			// // for (int i = 1; i < 6; i++) {
			// // Page page = Tool.deserializePage(table, i);
			// // System.out.println(page.toString());
			// // }

			// // SQLTerm[] arrSQLTerms;
			// // arrSQLTerms = new SQLTerm[2];
			// // for (int i = 0; i < arrSQLTerms.length; i++) {
			// // arrSQLTerms[i] = new SQLTerm();
			// // }
			// // arrSQLTerms[0]._strTableName = "Student";
			// // arrSQLTerms[0]._strColumnName = "gpa";
			// // arrSQLTerms[0]._strOperator = ">=";
			// // arrSQLTerms[0]._objValue = new Double(1.5);

			// // arrSQLTerms[1]._strTableName = "Student";
			// // arrSQLTerms[1]._strColumnName = "id";
			// // arrSQLTerms[1]._strOperator = "=";
			// // arrSQLTerms[1]._objValue = new Double(55);

			// // String[] strarrOperators = new String[1];
			// // strarrOperators[0] = "AND";
			// // // select * from Student where name = "John Noor" or gpa = 1.5;
			// // Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			// // while (resultSet.hasNext()) {
			// // System.out.println(resultSet.next());
			// // }

			// // String[]strarrOperators = new String[1];
			// // strarrOperators[0] = "OR";
			// // // select * from Student where name = "John Noor" or gpa = 1.5;
			// // Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
			// // while(resultSet.hasNext()){
			// // System.out.println(resultSet.next());
			// // }

			// // String[]strarrOperators = new String[1];
			// // strarrOperators[0] = "OR";
			// // // select * from Student where name = "John Noor" or gpa = 1.5;
			// // Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}
}
