package db2;

/** * @author Wael Abouelsaadat */ 

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm(){
		
	}
	public SQLTerm(String tableName, String columnName, String operator, Object value) {
		if (tableName == null || columnName == null || operator == null || value == null) {
		  throw new IllegalArgumentException("All arguments must be provided");
		}
		this._strTableName = tableName;
		this._strColumnName = columnName;
		this._strOperator = operator;
		this._objValue = value;
	  }

}