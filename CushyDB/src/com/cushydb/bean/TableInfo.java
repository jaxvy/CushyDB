package com.cushydb.bean;

import java.util.HashMap;
import java.util.Map;

import com.cushydb.common.CushyDBException;


public class TableInfo {

	//Properties
	private Map<String, String> tableAliasToTableNameMap;
	private String singleTableName;
	private boolean isSingleTable;
	
	//Constructors
	
	//For single table use
	private TableInfo( String singleTableName){		
		this.singleTableName = singleTableName;
		isSingleTable = true;
		tableAliasToTableNameMap = null;
	}
	
	//For multi table use
	private TableInfo(){
		isSingleTable = false;
		tableAliasToTableNameMap = new HashMap<String, String>();
	}
	
	//Factories
	public static TableInfo Single( String singleTableName){
		
		return new TableInfo( singleTableName);
	}
	
	public static TableInfo Multi(){
		
		return new TableInfo();
	}
	
	public void add( String tableName, String tableAlias){
		if( isSingleTable){
			throw new CushyDBException( "Cannot add a table name and table alias pair for a TableInfo for single table");
		}
		
		tableAliasToTableNameMap.put( tableAlias, tableName);
	}
	
	public boolean isSingleTable(){
		return isSingleTable;
	}
	
	public Map<String, String> getTableAliasToTableNameMap(){
		return tableAliasToTableNameMap;
	}
	
	public String getSingleTableName(){
		return singleTableName;
	}
	
	public String getSingleTableAlias(){
		return Parameter.SINGLE_TABLE_ALIAS;
	}
}
