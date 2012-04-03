package com.cushydb.test.mysql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cushydb.bean.Container;
import com.cushydb.bean.Join;
import com.cushydb.bean.Parameter;
import com.cushydb.bean.ParameterList;
import com.cushydb.bean.ParameterList.SetParameterList;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.enums.CompareType;
import com.cushydb.handler.mysql.MySQLUpdateHandler;

public class MySQLUpdateHandlerTest {
	
	private Connection databaseConnection;
	private CushyDBConnection cushyDBConnection;
	
	@Before
	public void setup() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException{
	
		CushyDBUtils.initLoj4J();
		
		Class.forName( "com.mysql.jdbc.Driver").newInstance();
		String connectionAddress = "jdbc:mysql://localhost:3306/cushydb_test?user=root&password=123456";		
		databaseConnection = DriverManager.getConnection(connectionAddress);						
		databaseConnection.setAutoCommit( false);		
		cushyDBConnection = new CushyDBConnection();
		cushyDBConnection.setJDBCConnection( databaseConnection);
		
		TestDBGenerator.createTables( databaseConnection);
		TestDBGenerator.insertTestData( databaseConnection);		
	}
	
	@After
	public void clean() throws SQLException{
		cushyDBConnection.rollbackTransaction();
		cushyDBConnection.close();
	}
	
	@Test
	public void test_1_sql(){
		
		TableInfo tableInfo = TableInfo.Single( "table1");
		
		SetParameterList setParameterList = ParameterList.Set();		
		setParameterList.add( Parameter.Set( "setParam1", 1))
						.add( Parameter.Set( "setParam2", "test"))
						.add( Parameter.Set( "setParam3", 123.45));
		
		MySQLUpdateHandler updateHandler = new MySQLUpdateHandler( cushyDBConnection);		
		String sql = updateHandler.Update( tableInfo)
					 			  .Set( setParameterList).sql();
		
		assertEquals(sql, "UPDATE table1 a SET a.setParam1 = ?, a.setParam2 = ?, a.setParam3 = ?");
	}
	
	@Test
	public void test_2_sql(){
		
		TableInfo tableInfo = TableInfo.Single("table1");
		
		SetParameterList setParameterList = ParameterList.Set();	
		setParameterList.add( Parameter.Set("setParam1", 1))
						.add( Parameter.Set("setParam2", "test"))
						.add( Parameter.Set("setParam3", 123.45));
		
		
		Container constraintContainer = Container.Or();
		constraintContainer.add( Parameter.Constraint( "param1", CompareType.GR, 123))
						   .add( Parameter.Constraint( "param2", CompareType.NOT_LIKE, "%123"));
		
		
		MySQLUpdateHandler updateHandler = new MySQLUpdateHandler( cushyDBConnection);		
		String sql = updateHandler.Update(tableInfo)
					 			  .Set(setParameterList)
					 			  .Where( constraintContainer).sql();
		
		assertEquals(sql, "UPDATE table1 a SET a.setParam1 = ?, a.setParam2 = ?, a.setParam3 = ? WHERE (a.param1 > ? OR a.param2 NOT LIKE ?)");						  
	}
	
	
	@Test
	public void test_3_exec() throws SQLException{
		
		TableInfo tableInfo = TableInfo.Single("student");
		
		SetParameterList setParameterList = ParameterList.Set();		
		setParameterList.add( Parameter.Set("firstname", "updated_firstname_1"));
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint("firstname", CompareType.LIKE, "firstname_1"));
		
		MySQLUpdateHandler updateHandler = new MySQLUpdateHandler( cushyDBConnection);
		updateHandler.Update( tableInfo)
					 .Set( setParameterList)
					 .Where( constraintContainer).execute();
		
		//Check using JDBC
		PreparedStatement preparedStatement = databaseConnection.prepareStatement("SELECT firstname FROM student WHERE lastname LIKE ?");
		preparedStatement.setString(1, "lastname_1");
		ResultSet resultSet = preparedStatement.executeQuery();
		if( resultSet.next()){
			String updatedFirstname = resultSet.getString(1);
			assertEquals(updatedFirstname, "updated_firstname_1");
		}
		else{
			throw new AssertionError();
		}			
	}
	
	@Test
	public void test_4_exec() throws SQLException{
		
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("course", "c");
		tableInfo.add("grade", "g");
		
		SetParameterList setParameterList = ParameterList.Set();		
		setParameterList.add( Parameter.Set( "g", "grade", "D"));
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "c", "name", CompareType.LIKE, "course_4"));
		constraintContainer.add( Join.Equals("c", "id", "g", "course_id"));
		
		MySQLUpdateHandler updateHandler = new MySQLUpdateHandler( cushyDBConnection);
		updateHandler.Update( tableInfo)
					 .Set( setParameterList)
					 .Where( constraintContainer).execute();
		
		//Check using JDBC
		PreparedStatement preparedStatement = databaseConnection.prepareStatement("SELECT COUNT(*) FROM grade WHERE grade LIKE ?");
		preparedStatement.setString(1, "D");
		ResultSet resultSet = preparedStatement.executeQuery();
		if( resultSet.next()){
			int count = resultSet.getInt(1);
			assertEquals(count, 8);
		}
		else{
			throw new AssertionError();
		}			
	}
}
