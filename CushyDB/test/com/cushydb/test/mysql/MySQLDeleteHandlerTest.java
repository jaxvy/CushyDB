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
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.enums.CompareType;
import com.cushydb.handler.mysql.MySQLDeleteHandler;

public class MySQLDeleteHandlerTest {
	
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
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("table1");
		
		Container constraintContainer = Container.And();
		constraintContainer.add(Parameter.Constraint("param1", CompareType.EQ, 11))
						   .add(Parameter.Constraint("param2", CompareType.LIKE, "12"))
						   .add(Parameter.Constraint("param3", CompareType.NEQ, 13));		
		
		//Perform action
		MySQLDeleteHandler deleteHandler = new MySQLDeleteHandler( cushyDBConnection);
		String sql = deleteHandler.Delete()
								  .From(tableInfo)
								  .Where(constraintContainer).sql();
		
		//Check
		assertEquals( sql, "DELETE a FROM table1 a WHERE (a.param1 = ? AND a.param2 LIKE ? AND a.param3 <> ?)");
		
	}
	
	@Test
	public void test_2_exec() throws SQLException{
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("grade");
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "grade", CompareType.LIKE, "A"));
		
		//Perform action
		MySQLDeleteHandler deleteHandler = new MySQLDeleteHandler( cushyDBConnection);
		deleteHandler.Delete()
					 .From(tableInfo)
					 .Where(constraintContainer).execute();
		
		
		//Check using JDBC
		PreparedStatement preparedStatement = databaseConnection.prepareStatement("SELECT COUNT(*) FROM GRADE WHERE GRADE LIKE ?");
		preparedStatement.setString(1, "A");
		ResultSet resultSet = preparedStatement.executeQuery();
		if( resultSet.next()){
			int count = resultSet.getInt(1);
			assertEquals(count, 0);
		}
		else{
			throw new AssertionError();
		}	
	}
	
	@Test(expected=CushyDBException.class)	//Exception due to foreign key constraint
	public void test_3_exec() throws SQLException{
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("course");
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "name", CompareType.LIKE, "course_1"));
		
		//Perform action
		MySQLDeleteHandler deleteHandler = new MySQLDeleteHandler( cushyDBConnection);
		deleteHandler.Delete()
					 .From(tableInfo)
					 .Where(constraintContainer).execute();	
		
	}
	
	@Test
	public void test_4_exec() throws SQLException{
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("course", "c");
		tableInfo.add("grade", "g");
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "c", "name", CompareType.LIKE, "course_1"));
		constraintContainer.add( Join.Equals( "c", "id", "g", "course_id"));
		
		//Perform action
		MySQLDeleteHandler deleteHandler = new MySQLDeleteHandler( cushyDBConnection);
		deleteHandler.Delete( "g")
					 .From( tableInfo)
					 .Where( constraintContainer).execute();	
		
		//Check using JDBC
		PreparedStatement preparedStatement = databaseConnection.prepareStatement("SELECT COUNT(*) FROM GRADE G, COURSE C WHERE C.NAME LIKE ? AND G.COURSE_ID = C.ID");
		preparedStatement.setString(1, "course_1");
		ResultSet resultSet = preparedStatement.executeQuery();
		if( resultSet.next()){
			int count = resultSet.getInt(1);
			assertEquals(count, 0);
		}
		else{
			throw new AssertionError();
		}			
	}
	
}
