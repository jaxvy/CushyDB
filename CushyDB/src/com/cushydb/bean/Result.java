package com.cushydb.bean;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.cushydb.bean.ParameterList.SelectParameterList;
import com.cushydb.bean.Row.Column;
import com.cushydb.common.CushyDBException;

public class Result {

	//Properties
	private Long generatedKeyValue;
	private List<Row> rowList;
	
		
	//Methods
	public void importGeneratedKeyJDBC( ResultSet resultSet) {
		
		try{
			if( resultSet.next()){	
				generatedKeyValue = resultSet.getLong(1);		
			}	
			else{
				generatedKeyValue = null;
			}
		}
		catch( SQLException e){
			throw new CushyDBException("An exception occurred while retrieving generated keys", e);
		}
	}
		
	
	public long getGeneratedKey() {
		return generatedKeyValue;
	}
		
		
	public void importResultSet( ResultSet resultSet, SelectParameterList selectParameterList) throws CushyDBException{
			
		rowList = new ArrayList<Row>();
		
		try{
			while( resultSet.next()){
			
				Row row = new Row();
			
				ResultSetMetaData metaData = resultSet.getMetaData();
				for( int i = 1; i <= metaData.getColumnCount(); i++){
					
					String parameterName = metaData.getColumnLabel(i);
					Object parameterValue = resultSet.getObject( parameterName);
					Column column = new Column( parameterName.toLowerCase(Locale.ENGLISH), parameterValue);
					row.addColumn( column);
				}
				
				
				rowList.add( row);
			}
		}
		catch( SQLException e){
			throw new CushyDBException( "An exception occurred while converting resultSet to List<Row>", e);
		}
	}
	
	public List<Row> getRowList(){
		return rowList;
	}
}
