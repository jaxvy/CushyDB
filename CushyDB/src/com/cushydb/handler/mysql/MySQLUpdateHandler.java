package com.cushydb.handler.mysql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cushydb.bean.Container;
import com.cushydb.bean.ParameterList.SetParameterList;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.handler.DBConnectionHandler;
import com.cushydb.handler.DBUpdateHandler;

public class MySQLUpdateHandler implements DBUpdateHandler{

	//Properties
	private CushyDBConnection cushyDBConnection;
	private static final Logger logger = Logger.getLogger( MySQLUpdateHandler.class);
	
	private TableInfo tableInfo;
	private SetParameterList setParameterList;
	private Container constraintContainer;
		
	//Constructor
	public MySQLUpdateHandler( CushyDBConnection cushyDBConnection){
		this.cushyDBConnection = cushyDBConnection;		
	}
	

	@Override
	public DBUpdateHandler Update( TableInfo tableInfo){
		this.tableInfo = tableInfo;
		return this;
	}
	
	@Override
	public DBUpdateHandler Set( SetParameterList setParameterList){
		this.setParameterList = setParameterList;
		return this;
	}

	@Override
	public DBUpdateHandler Where( Container constraintContainer){
		this.constraintContainer = constraintContainer;
		return this;
	}
	
	@Override
	public boolean execute(){
		
		return execute( setParameterList, constraintContainer, tableInfo);
	}
	
	@Override
	public String sql(){
		
		return constructQueryString( setParameterList, constraintContainer, tableInfo, new ArrayList<Object>());
	}
	
	private boolean execute( SetParameterList setParameterList, Container constraintContainer, TableInfo tableInfo) {
		
		boolean isActionSuccessful = false;
		PreparedStatement preparedStatement = constructPreparedStatement( setParameterList,	constraintContainer, tableInfo);
		
		try{			
			preparedStatement.executeUpdate();
			isActionSuccessful = true;
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while executing update", e);			
		}
		finally{
			DBConnectionHandler.close( preparedStatement);
		}
						
		return isActionSuccessful;
	}

	private PreparedStatement constructPreparedStatement( SetParameterList setParameterList, Container constraintContainer, TableInfo tableInfo){
		
		List<Object> parameterValueListInSequence = new ArrayList<Object>();
		String sql = constructQueryString( setParameterList, constraintContainer, tableInfo, parameterValueListInSequence);
		
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = cushyDBConnection.prepareStatement( sql);
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while constructing a PreparedStatement from SQL: " + sql, e);
		}
		
		CushyDBUtils.bindWithObjectList( preparedStatement, parameterValueListInSequence);
		
		return preparedStatement;
	}
	
	private String constructQueryString( SetParameterList setParameterList, Container constraintContainer, 
										 TableInfo tableInfo, List<Object> parameterValueListInSequence){
		
		StringBuilder stringBuilder = new StringBuilder(""); //Update table1 t1, table2 t2 set t1.a = 1, t2.b = 4, t1.c = t2.c WHERE t1.x = 2 AND ...
		
		generateUpdateClause( tableInfo, stringBuilder);	
		
		generateSetClause( setParameterList, stringBuilder, parameterValueListInSequence);
		
		generateWhereClause( constraintContainer, stringBuilder, parameterValueListInSequence);
		
		logger.debug("Generated SQL: " + stringBuilder.toString());
		
		return stringBuilder.toString();
	}
	
	private void generateUpdateClause( TableInfo tableInfo, StringBuilder stringBuilder){
			
		if( !tableInfo.isSingleTable() && (tableInfo.getTableAliasToTableNameMap() == null || tableInfo.getTableAliasToTableNameMap().isEmpty())){
			throw new CushyDBException("No table name and alias is defined");
		}
		
		stringBuilder.append( "UPDATE ");
		
		MySQLQueryBuilder.generateFromOrUpdateClause( tableInfo, stringBuilder);
	}
	
	private void generateSetClause( SetParameterList setParameterList, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		if( setParameterList == null || setParameterList.isEmpty()){
			throw new CushyDBException("No set parameters are defined");
		}
		
		stringBuilder.append(" SET ");
		
		MySQLQueryBuilder.generateSetClause( setParameterList, stringBuilder, parameterValueListInSequence);
	}
	
	private void generateWhereClause( Container constraintContainer, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		if( constraintContainer != null){
			stringBuilder.append(" WHERE ");
			
			MySQLQueryBuilder.generateWhereOrHavingClause( constraintContainer, stringBuilder, parameterValueListInSequence);
		}
		
	}
}
