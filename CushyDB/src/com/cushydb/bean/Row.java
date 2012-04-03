package com.cushydb.bean;


import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


public class Row {
	
	private Map<String, Column> columnMap;
	
	public Row(){
		columnMap = new HashMap<String, Column>();
	}
	
	
	public void addColumn( Column column){
		
		columnMap.put( column.getColumnName(), column);
	}
	
	
	@SuppressWarnings("unchecked")
	public <T> T getColumn( String columnName){
		
		return (T) columnMap.get( columnName.toLowerCase(Locale.ENGLISH)).getColumnValue();
	}
	
	static class Column{
				
		private String columnName;
		private Object columnValue;
		
		public Column(String columnName, Object columnValue) {
			this.columnName = columnName;
			this.columnValue = columnValue;
		}

		
		public String getColumnName() {
			return columnName;
		}
		
		public Object getColumnValue() {
			return columnValue;
		}		
	}
}
