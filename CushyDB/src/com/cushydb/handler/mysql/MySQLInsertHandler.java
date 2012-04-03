package com.cushydb.handler.mysql;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.hamcrest.core.IsEqual;

import com.cushydb.bean.Parameter.InsertParameter;
import com.cushydb.bean.ParameterInterface;
import com.cushydb.bean.ParameterList.InsertParameterList;
import com.cushydb.bean.Result;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.common.CushyDBValidator;
import com.cushydb.handler.DBConnectionHandler;
import com.cushydb.handler.DBInsertHandler;

public class MySQLInsertHandler implements DBInsertHandler{

	private CushyDBConnection cushyDBConnection;
	private static final Logger logger = Logger.getLogger( MySQLInsertHandler.class);
	
	private InsertParameterList insertParameterList;
	private List<Object[]> batchInsertList;
	private boolean isInsertBatch;
	private String tableName;
	private boolean isReturnKey;
	private int batchSize;
	
	public static final int BATCH_SIZE_DEFAULT = 1000;
	
	//Constructor
	public MySQLInsertHandler( CushyDBConnection cushyDBConnection){
		this.cushyDBConnection = cushyDBConnection;	
		isReturnKey = false;
		batchSize = BATCH_SIZE_DEFAULT;
	}
		
		
	//Methods
	@Override
	public DBInsertHandler Insert( InsertParameterList insertParameterList) {
		this.insertParameterList = insertParameterList;
		isInsertBatch = false;
		return this;
	}
	
	@Override
	public DBInsertHandler AddBatch( Object[] batchInsertParameters) {
		if( batchInsertList == null){
			batchInsertList = new ArrayList<Object[]>();
			isInsertBatch = true;
		}
		batchInsertList.add( batchInsertParameters);				
		return this;
	}

	
	
	@Override
	public DBInsertHandler Into( String tableName) {
		this.tableName = tableName;
		return this;
	}

	@Override
	public DBInsertHandler ReturnKey(){
		isReturnKey = true;
		return this;
	}
	
	@Override
	public DBInsertHandler BatchSize( int batchSize){
		this.batchSize = batchSize;
		return this;
	}
	
	
	@Override
	public Result execute() {
		
		if( !isInsertBatch){
			return execute( insertParameterList, tableName);
		}
		else{
			return executeBatch( insertParameterList, batchInsertList, tableName);
		}
				
	}
	
	@Override
	public String queryString(){
		
		return constructQueryString( insertParameterList, tableName);				
	}

	private Result execute( InsertParameterList insertParameterList, String tableName){
		
		Result result = new Result();		
		
		if( tableName == null){
			throw new CushyDBException("No table name is registered");
		}
				
		
		PreparedStatement preparedStatement = constructPreparedStatement( insertParameterList, tableName);
		
		CushyDBUtils.bindWithInsertParameterList( preparedStatement, insertParameterList.getList());
		
		//Execute prepared statement
		ResultSet resultSet = null;
		try{
						
			preparedStatement.executeUpdate();
			
			if( isReturnKey){
				resultSet = preparedStatement.getGeneratedKeys();				
				result.importGeneratedKeyJDBC( resultSet);
			}
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while executing insert", e);
		}
		finally{			
			DBConnectionHandler.close( resultSet, preparedStatement);
		}
		
		return result;
	}
		
	
	private PreparedStatement constructPreparedStatement( InsertParameterList parameterList, String tableName){
		
		String sql = constructQueryString( parameterList, tableName);
		
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = cushyDBConnection.prepareStatement( sql);
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while constructing PreparedStatement with SQL: " + sql, e);			
		}
				
		return preparedStatement;
	}
	
	private String constructQueryString( InsertParameterList insertParameterList, String tableName){
		
		CushyDBValidator.hasUniqueParameterNames( insertParameterList);	
		
		StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");
		stringBuilder.append( tableName);
		stringBuilder.append(" (");
		
		StringBuilder questionMarkStrB = new StringBuilder("");
		for( ParameterInterface p: insertParameterList.getList()){
		
			InsertParameter insertParameter = (InsertParameter)p;
			
			stringBuilder.append(insertParameter.getParameterName());
			stringBuilder.append(", ");
			
			questionMarkStrB.append("?,");
		}
		
		stringBuilder = stringBuilder.delete(stringBuilder.length()-2, stringBuilder.length());
		questionMarkStrB = questionMarkStrB.delete( questionMarkStrB.length()-1, questionMarkStrB.length());
		
		stringBuilder.append(") VALUES (");
		stringBuilder.append( questionMarkStrB);		
		stringBuilder.append(')');
		
		logger.debug("Generated SQL: " + stringBuilder.toString());
		
		return stringBuilder.toString();
	}	
	
	private Result executeBatch( InsertParameterList insertParameterList, List<Object[]> batchInsertList, String tableName){
		
		Result result = new Result();		
		
		if( tableName == null){
			throw new CushyDBException("No table name is registered");
		}
		
		//Construct a PreparedStatement using the first insertParameterList
		PreparedStatement preparedStatement = constructPreparedStatement( insertParameterList, tableName);
		CushyDBUtils.bindWithInsertParameterList( preparedStatement, insertParameterList.getList());
		
		ResultSet resultSet = null;
		try{
			
			preparedStatement.addBatch();
			
			int count = 1;
			for( Object[] batchInsertParameters: batchInsertList){
				
				CushyDBUtils.bindWithObjectArray( preparedStatement, batchInsertParameters);
				preparedStatement.addBatch();
				
				if(++count % batchSize == 0) {
					preparedStatement.executeBatch();
			    }			
			}
			
			preparedStatement.executeBatch();			
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while executing insert", e);
		}
		finally{			
			DBConnectionHandler.close( resultSet, preparedStatement);
		}
		
		
		
		return result;
	}
}


	
