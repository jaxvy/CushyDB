package com.cushydb.test.mysql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import junit.framework.AssertionFailedError;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cushydb.bean.Parameter;
import com.cushydb.bean.ParameterList;
import com.cushydb.bean.ParameterList.InsertParameterList;
import com.cushydb.bean.Result;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.handler.mysql.MySQLInsertHandler;

public class MySQLInsertHandlerTest{
		
	private static CushyDBConnection cushyDBConnection;
	
	@BeforeClass
	public static void setup() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException{
	
		CushyDBUtils.initLoj4J();
		
		Class.forName( "com.mysql.jdbc.Driver").newInstance();
		String connectionAddress = "jdbc:mysql://localhost:3306/cushydb_test?user=root&password=123456";		
		Connection databaseConnection = DriverManager.getConnection(connectionAddress);						
		databaseConnection.setAutoCommit( false);		
		cushyDBConnection = new CushyDBConnection();
		cushyDBConnection.setJDBCConnection( databaseConnection);
		
		Statement statement = databaseConnection.createStatement();				
		statement.executeUpdate("drop table if exists TABLE_NAME;");
		
		String sql = "CREATE TABLE TABLE_NAME (ID INT NOT NULL AUTO_INCREMENT, PARAM1 VARCHAR(64), PARAM2 DOUBLE, PARAM3 DATETIME, PARAM4 INT, PARAM5 LONG, PRIMARY KEY (ID))";
		statement.executeUpdate(sql);
		
	}
	
	@AfterClass
	public static void clean() throws SQLException{
		cushyDBConnection.rollbackTransaction();
		cushyDBConnection.close();
	}	
	
	@Test	
	public void test_1_exec(){
			
		//Prepare data
		String tableName = "TABLE_NAME";
		
		InsertParameterList insertParameterList = ParameterList.Insert();
		insertParameterList.add( Parameter.Insert("param1", "val1"))
					   	   .add( Parameter.Insert("param2", 1.02))
					   	   .add( Parameter.Insert("param3", new Timestamp( System.currentTimeMillis())))
					   	   .add( Parameter.Insert("param4", 12))
					   	   .add( Parameter.Insert("param5", 14L));
		
		
		//Perform action
		MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
		Result result = insertHandler.Insert( insertParameterList)
								     .Into( tableName)
								     .ReturnKey().execute();
		
		//Check results
		int generatedKey = (int)result.getGeneratedKey();
		assertEquals( generatedKey, 1);
	}
	
	@Test(expected=CushyDBException.class)	
	public void test_2_sql(){
				
		//Prepare data
		String tableName = "TABLE_NAME";
		
		InsertParameterList insertParameterList = ParameterList.Insert();
		insertParameterList.add( Parameter.Insert("param1", "val1"))
					   	   .add( Parameter.Insert("param2", 1.02))
					   	   .add( Parameter.Insert("param4", new Timestamp( System.currentTimeMillis())))
					   	   .add( Parameter.Insert("param4", 12));
		
		
		//Perform action
		MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
		insertHandler.Insert( insertParameterList)
		  			 .Into( tableName).queryString();	//ExceptedException: CushyDBException						 				 
	}	
	
	@Test(expected=CushyDBException.class)	
	public void test_3_sql(){
				
		//Prepare data
		String tableName = "TABLE_NAME";
		
		InsertParameterList insertParameterList = ParameterList.Insert();
		insertParameterList.add( Parameter.Insert(" ", "val1"))
					   	   .add( Parameter.Insert("param2", 1.02))
					   	   .add( Parameter.Insert("param4", new Timestamp( System.currentTimeMillis())))
					   	   .add( Parameter.Insert("param4", 12));
		
		
		//Perform action
		MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
		insertHandler.Insert( insertParameterList)
		  			 .Into( tableName).queryString();	//ExceptedException: CushyDBException					 				 
	}
	
	@Test	
	public void test_4_exec() throws SQLException{
			
		//Prepare data
		String tableName = "TABLE_NAME";
				
		InsertParameterList baseInsertParameterList = ParameterList.Insert();
		baseInsertParameterList.add( Parameter.Insert("param1", "val"))
						   	   .add( Parameter.Insert("param2", 0))
						   	   .add( Parameter.Insert("param3", new Timestamp( System.currentTimeMillis())))
						   	   .add( Parameter.Insert("param4", 0))
						   	   .add( Parameter.Insert("param5", 0L));
					
		MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
		insertHandler.Insert( baseInsertParameterList)
				     .Into( tableName)
				     .ReturnKey();
		
		
		for( int i = 1; i <= 9; i++){
			
			Object[] batchInsertParameters = {"val" + i, i, new Timestamp( System.currentTimeMillis()), i, i};
			insertHandler.AddBatch( batchInsertParameters);				
		}
		
		//Perform action
		insertHandler.BatchSize(100)
					 .execute();	//Batch insert does not provide returning of generated keys
				
		//Check results
		PreparedStatement preparedStatement = cushyDBConnection.prepareStatement("SELECT COUNT(*) FROM TABLE_NAME");
		ResultSet result = preparedStatement.executeQuery();
		if( result.next()){
			int count = result.getInt(1);
			assertEquals(count, 11);
		}
		else{
			throw new AssertionFailedError();
		}		
	}
	
	@Test	
	public void test_5_exec(){
			
		//Prepare data
		String tableName = "TABLE_NAME";
		
		InsertParameterList insertParameterList = ParameterList.Insert();
		insertParameterList.add( "param1", "val1")
					   	   .add( "param2", 1.02)
					   	   .add( "param3", new Timestamp( System.currentTimeMillis()))
					   	   .add( "param4", 12)
					   	   .add( "param5", 14L);
		
		
		//Perform action
		MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
		Result result = insertHandler.Insert( insertParameterList)
								     .Into( tableName)
								     .ReturnKey().execute();
		
		//Check results
		int generatedKey = (int)result.getGeneratedKey();
		assertEquals( generatedKey, 12);
	}
}
