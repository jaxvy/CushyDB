package com.cushydb.handler.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cushydb.bean.Container;
import com.cushydb.bean.Parameter;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.handler.DBConnectionHandler;
import com.cushydb.handler.DBDeleteHandler;

public class MySQLDeleteHandler implements DBDeleteHandler{

	private CushyDBConnection cushyDBConnection;
	private static final Logger logger = Logger.getLogger( MySQLDeleteHandler.class);
	
	private String deleteTableAlias;
	private TableInfo tableInfo;
	private Container constraintContainer;
	
	//Constructor
	public MySQLDeleteHandler( CushyDBConnection cushyDBConnection){
		this.cushyDBConnection = cushyDBConnection;		
	}

	@Override
	public DBDeleteHandler Delete( String deleteTableAlias){
		this.deleteTableAlias = deleteTableAlias;
		return this;
	}
	
	@Override
	public DBDeleteHandler Delete(){
		this.deleteTableAlias = Parameter.SINGLE_TABLE_ALIAS;
		return this;
	}
	
	@Override
	public DBDeleteHandler From( TableInfo tableInfo){
		this.tableInfo = tableInfo;
		return this;	
	}
	
	@Override
	public DBDeleteHandler Where( Container constraintContainer){
		this.constraintContainer = constraintContainer;
		return this;
	}
	
	@Override
	public boolean execute(){
		
		return execute( deleteTableAlias, tableInfo, constraintContainer);
	}

	@Override
	public String sql(){
		
		return constructQueryString( deleteTableAlias, tableInfo, constraintContainer, new ArrayList<Object>());
	}
	
	private boolean execute( String deleteTableAlias, TableInfo tableInfo, Container constraintContainer){
		
		boolean isActionSuccessful = false;
		PreparedStatement preparedStatement = constructPreparedStatement( deleteTableAlias, tableInfo, constraintContainer);
		
		try{
			preparedStatement.executeUpdate();	
			isActionSuccessful = true;
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while executing database query", e);
		}
		finally{
			DBConnectionHandler.close( preparedStatement);
		}				
		
		return isActionSuccessful;
	}

	private PreparedStatement constructPreparedStatement( String deleteTableAlias, TableInfo tableInfo, Container constraintContainer){
		
		List<Object> parameterValueListInSequence = new ArrayList<Object>();
		
		String sql = constructQueryString( deleteTableAlias, tableInfo, constraintContainer, parameterValueListInSequence);
		
		PreparedStatement preparedStatement;
		try{
			preparedStatement = cushyDBConnection.prepareStatement( sql);			
		}
		catch (SQLException e) {
			throw new CushyDBException("An exception occurred while constructing preparedStatement with sql: " + sql, e);
		}
		
		CushyDBUtils.bindWithObjectList( preparedStatement, parameterValueListInSequence);
		
		return preparedStatement;
	}
	
	private String constructQueryString( String deleteTableAlias, TableInfo tableInfo, Container constraintContainer, List<Object> parameterValueListInSequence){
		
		StringBuilder stringBuilder = new StringBuilder("");
		
		generateDeleteClause( stringBuilder, deleteTableAlias);
		
		generateFromClause( stringBuilder, tableInfo);
		
		generateWhereClause( stringBuilder, constraintContainer, parameterValueListInSequence);
				
		logger.debug("Generated SQL: " + stringBuilder.toString());
		
		return stringBuilder.toString();
	}

	private void generateDeleteClause( StringBuilder stringBuilder, String deleteTableAlias){
		
		if( deleteTableAlias == null){
			throw new CushyDBException("No delete table alias specified");
		}
		
		stringBuilder.append("DELETE " + deleteTableAlias);		
	}
	
	private void generateFromClause( StringBuilder stringBuilder, TableInfo tableInfo){
		
		if( !tableInfo.isSingleTable() && (tableInfo.getTableAliasToTableNameMap() == null || tableInfo.getTableAliasToTableNameMap().isEmpty())){
			throw new CushyDBException("No delete table alias to table name map specified");
		}
		
		stringBuilder.append(" FROM ");
		
		MySQLQueryBuilder.generateFromOrUpdateClause( tableInfo, stringBuilder);
	}

	private void generateWhereClause( StringBuilder stringBuilder, Container constraintContainer, List<Object> parameterValueListInSequence){
		
		if( constraintContainer != null){
			stringBuilder.append( " WHERE ");
			
			MySQLQueryBuilder.generateWhereOrHavingClause( constraintContainer, stringBuilder, parameterValueListInSequence);
		}
	}
}
