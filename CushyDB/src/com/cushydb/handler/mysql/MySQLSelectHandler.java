package com.cushydb.handler.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import com.cushydb.bean.Container;
import com.cushydb.bean.ParameterList.GroupByParameterList;
import com.cushydb.bean.ParameterList.OrderByParameterList;
import com.cushydb.bean.ParameterList.SelectParameterList;
import com.cushydb.bean.Result;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.handler.DBConnectionHandler;
import com.cushydb.handler.DBSelectHandler;

public class MySQLSelectHandler implements DBSelectHandler{

	//Properties
	private CushyDBConnection cushyDBConnection;
	private static final Logger logger = Logger.getLogger( MySQLSelectHandler.class);
	
	private SelectParameterList selectParameterList;
	private boolean isDistinct;
	private TableInfo tableInfo;
	private Container constraintContainer;
	private GroupByParameterList groupByParameterList;
	private Container havingContainer;
	private OrderByParameterList orderByParameterList;
	private int limit = INVALID_LIMIT;
	private int offset = INVALID_OFFSET;
	
	
	public static final int INVALID_OFFSET = -1;
	public static final int INVALID_LIMIT = -1;
	
	//Constructor
	public MySQLSelectHandler( CushyDBConnection cushyDBConnection){
		this.cushyDBConnection = cushyDBConnection;		
	}
	
	//Methods
	
	@Override
	public DBSelectHandler Select( SelectParameterList selectParameterList) {
		this.selectParameterList = selectParameterList;
		isDistinct = false;
		return this;
	}
	
	@Override
	public DBSelectHandler Distinct() {
		this.isDistinct = true;
		return this;
	}

	@Override
	public DBSelectHandler From( TableInfo tableInfo) {
		this.tableInfo = tableInfo;
		return this;
	}

	@Override
	public DBSelectHandler Where( Container constraintContainer) {
		this.constraintContainer = constraintContainer;
		return this;
	}

	@Override
	public DBSelectHandler GroupBy( GroupByParameterList groupByParameterList) {
		this.groupByParameterList = groupByParameterList;
		return this;
	}

	@Override
	public DBSelectHandler Having( Container havingContainer) {
		this.havingContainer = havingContainer;
		return this;
	}

	@Override
	public DBSelectHandler OrderBy( OrderByParameterList orderByParameterList) {
		this.orderByParameterList = orderByParameterList;
		return this;
	}

	@Override
	public DBSelectHandler Limit( int limit) {
		this.limit = limit;
		return this;
	}

	@Override
	public DBSelectHandler Limit( int limit, int offset) {
		this.limit = limit;
		this.offset = offset;
		return this;
	}

	@Override
	public Result execute() {
		
		return execute( selectParameterList, isDistinct, constraintContainer, tableInfo, groupByParameterList, 
						havingContainer, orderByParameterList, limit, offset);
	}
	
	@Override
	public String sql(){
		
		return constructQueryString( selectParameterList, isDistinct, constraintContainer, tableInfo, groupByParameterList, 
							  		 havingContainer, orderByParameterList, limit, offset, new ArrayList<Object>());
		
	}
	
	
	private Result execute( SelectParameterList selectParameterList, boolean isDistinct, Container constraintContainer, TableInfo tableInfo,
							GroupByParameterList groupByParameterList, Container havingContainer, OrderByParameterList orderByParameterList, int limit, int offset){
		
		Result resultContainer = new Result();
		
		PreparedStatement preparedStatement = constructPreparedStatement( selectParameterList, isDistinct, constraintContainer, tableInfo, groupByParameterList,
																		  havingContainer, orderByParameterList, limit, offset);
		
		ResultSet resultSet = null;
		try{			
			resultSet = preparedStatement.executeQuery();
			resultContainer.importResultSet( resultSet, selectParameterList);
		}
		catch( SQLException e){
			throw new CushyDBException( "An exception has occurred while executing constructed preparedStatement", e);
		}
		finally{
			DBConnectionHandler.close( resultSet, preparedStatement);
		}
				
		return resultContainer;
	}
	
	private PreparedStatement constructPreparedStatement( SelectParameterList selectParameterList, boolean isDistinct, Container constraintContainer,
														  TableInfo tableInfo, GroupByParameterList groupByParameterList,
														  Container havingContainer, OrderByParameterList orderByParameterList, int limit, int offset){
		
		List<Object> parameterValueListInSequence = new ArrayList<Object>();
		
		String sql = constructQueryString( selectParameterList, isDistinct, constraintContainer, tableInfo, groupByParameterList, 
										   havingContainer, orderByParameterList, limit, offset, parameterValueListInSequence);
				
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = cushyDBConnection.prepareStatement( sql);
		} 
		catch (SQLException e){
			throw new CushyDBException("An exception has occurred while constructing a PreparedStatement from SQL: " + sql, e);
		}
		
		CushyDBUtils.bindWithObjectList( preparedStatement, parameterValueListInSequence);		
							
		return preparedStatement;
	}
	
	private String constructQueryString( SelectParameterList selectParameterList, boolean isDistinct, 
										 Container constraintContainer, 
										 TableInfo tableInfo, 
										 GroupByParameterList groupByParameterList, Container havingContainer,
										 OrderByParameterList orderByParameterList, int limit, int offset,
										 List<Object> parameterValueListInSequence){
						
		StringBuilder stringBuilder = new StringBuilder("");
		
		generateSelectClause( selectParameterList, stringBuilder, isDistinct);
		
		generateFromClause( tableInfo, stringBuilder);
		
		generateWhereClause( constraintContainer, stringBuilder, parameterValueListInSequence);
		
		generateGroupByClause( groupByParameterList, stringBuilder);
		
		generateHavingClause( havingContainer, stringBuilder, parameterValueListInSequence);
		
		generateOrderByClause( orderByParameterList, stringBuilder);
		
		generateLimitOffsetClause( limit, offset, stringBuilder);
		
		logger.debug("Generated SQL: " + stringBuilder.toString());
		
		return stringBuilder.toString();
	}
	
	private void generateSelectClause( SelectParameterList selectParameterList, StringBuilder stringBuilder, boolean isDistinct){
		
		String selectClause = "SELECT ";
		if( isDistinct){
			selectClause += "DISTINCT ";
		}
		
		if( selectParameterList == null || selectParameterList.isEmpty()){
			stringBuilder.append( selectClause + "*");
		}
		else{
			stringBuilder.append(selectClause);
			
			MySQLQueryBuilder.generateSelectClause( selectParameterList, stringBuilder);
		}		
	}
	
	private void generateFromClause( TableInfo tableInfo, StringBuilder stringBuilder){
		
		stringBuilder.append( " FROM ");
		
		MySQLQueryBuilder.generateFromOrUpdateClause( tableInfo, stringBuilder);
	}
		
	private void generateWhereClause( Container constraintContainer, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		if( constraintContainer != null){
			stringBuilder.append(" WHERE ");
		
			MySQLQueryBuilder.generateWhereOrHavingClause( constraintContainer, stringBuilder, parameterValueListInSequence);
		}
	}
	
	private void generateGroupByClause( GroupByParameterList groupByParameterList, StringBuilder stringBuilder){
		
		if( groupByParameterList != null){
			stringBuilder.append(" GROUP BY ");
			
			MySQLQueryBuilder.generateGroupByClause( groupByParameterList, stringBuilder);
		}		
	}
	
	private void generateHavingClause( Container havingContainer, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		if( havingContainer != null){
			
			stringBuilder.append(" HAVING ");
			
			MySQLQueryBuilder.generateWhereOrHavingClause( havingContainer, stringBuilder, parameterValueListInSequence);
		}
		
	}
	
	private void generateOrderByClause( OrderByParameterList orderByParameterList, StringBuilder stringBuilder){
		
		if( orderByParameterList != null){
			stringBuilder.append(" ORDER BY ");
			
			MySQLQueryBuilder.generateOrderByClause( orderByParameterList, stringBuilder);
		}		
	}
	
	private void generateLimitOffsetClause( int limit, int offset, StringBuilder stringBuilder){
		
		if( limit != INVALID_LIMIT){
			stringBuilder.append(" LIMIT " + limit);
			
			if( offset != INVALID_OFFSET){
				stringBuilder.append( " , " + offset);
			}
		}
	}
}
