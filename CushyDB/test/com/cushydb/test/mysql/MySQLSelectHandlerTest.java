package com.cushydb.test.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cushydb.bean.Container;
import com.cushydb.bean.Join;
import com.cushydb.bean.Parameter;
import com.cushydb.bean.ParameterList;
import com.cushydb.bean.ParameterList.GroupByParameterList;
import com.cushydb.bean.ParameterList.OrderByParameterList;
import com.cushydb.bean.ParameterList.SelectParameterList;
import com.cushydb.bean.Result;
import com.cushydb.bean.Row;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.enums.CompareType;
import com.cushydb.enums.FunctionType;
import com.cushydb.handler.mysql.MySQLSelectHandler;

public class MySQLSelectHandlerTest {

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

		TestDBGenerator.createTables( databaseConnection);
		TestDBGenerator.insertTestData( databaseConnection);
		
	}
	
	@AfterClass
	public static void clean() throws SQLException{
		cushyDBConnection.rollbackTransaction();
		cushyDBConnection.close();
	}
	
	
	@Test
	public void test_1_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
	
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.From(tableInfo).sql();
				
		//Check result
		assertEquals(sql, "SELECT * FROM table1 a");
	}
	
	@Test
	public void test_2_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
								  .From(tableInfo).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a");
	}
	
	@Test
	public void test_3_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?)");
	}
	
	@Test
	public void test_4_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "groupByParam"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer)
		  						  .GroupBy(groupByParameterList).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?) GROUP BY a.groupByParam ASC");
	}
	
	@Test
	public void test_5_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "groupByParam"));
		
		Container havingContainer = Container.And();
		havingContainer.add( Parameter.Constraint( "havingParam", CompareType.EQ, "test"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer)
		  						  .GroupBy(groupByParameterList)
		  						  .Having(havingContainer).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?) GROUP BY a.groupByParam ASC HAVING (a.havingParam = ?)");
	}
	
	@Test
	public void test_6_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "groupByParam"));
		
		Container havingContainer = Container.And();
		havingContainer.add( Parameter.Constraint( "havingParam", CompareType.EQ, "test"));
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByDesc( "orderByParam"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer)
		  						  .GroupBy(groupByParameterList)
		  						  .Having(havingContainer)
		  						  .OrderBy(orderByParameterList).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?) " +
						  "GROUP BY a.groupByParam ASC HAVING (a.havingParam = ?) ORDER BY a.orderByParam DESC");
	}
	
	@Test
	public void test_7_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "groupByParam"));
		
		Container havingContainer = Container.And();
		havingContainer.add( Parameter.Constraint( "havingParam", CompareType.EQ, "test"));
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByDesc( "orderByParam"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer)
		  						  .GroupBy(groupByParameterList)
		  						  .Having(havingContainer)
		  						  .OrderBy(orderByParameterList)
		  						  .Limit(10).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?) " +
						  "GROUP BY a.groupByParam ASC HAVING (a.havingParam = ?) ORDER BY a.orderByParam DESC LIMIT 10");
	}
	
	@Test
	public void test_8_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "selectParam1"));
			
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint( "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint( "intParam", CompareType.GR, 20));
			
		
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "groupByParam"));
		
		Container havingContainer = Container.And();
		havingContainer.add( Parameter.Constraint( "havingParam", CompareType.EQ, "test"));
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByDesc( "orderByParam"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer)
		  						  .GroupBy(groupByParameterList)
		  						  .Having(havingContainer)
		  						  .OrderBy(orderByParameterList)
		  						  .Limit(10, 20).sql();
				
		//Check result
		assertEquals(sql, "SELECT a.selectParam1 FROM table1 a WHERE (a.strParam = ? AND a.intParam > ?) " +
						  "GROUP BY a.groupByParam ASC HAVING (a.havingParam = ?) ORDER BY a.orderByParam DESC LIMIT 10 , 20");
	}
	
	@Test
	public void test_9_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("table1", "T1");
		tableInfo.add("table2", "T2");
		tableInfo.add("table3", "T3");
						
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select( "T1", "selectParam1"));
		selectParameterList.add( Parameter.Select( "T2", "selectParam2"));
		selectParameterList.add( Parameter.Select( "T3", "selectParam3"));
			
		Container c1 = Container.Or();
		c1.add( Parameter.Constraint("T3", "p3", CompareType.EQ, 20L));
		c1.add( Parameter.Constraint("T2", "p4", CompareType.IS_NOT, null));
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint("T1", "strParam", CompareType.EQ, "test"))
						   .add( Parameter.Constraint("T1", "intParam", CompareType.GR, 20))
						   .add( Join.Equals("T2" , "p5", "T3", "p6"))
						   .add( c1);	
		
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Select(selectParameterList)
		  						  .From(tableInfo)
		  						  .Where(constraintContainer).sql();
				
		//Check result
		assertTrue( sql.startsWith("SELECT T1.selectParam1, T2.selectParam2, T3.selectParam3"));
		assertTrue( sql.contains("table1 T1"));
		assertTrue( sql.contains("table2 T2"));
		assertTrue( sql.contains("table3 T3"));
		
		String whereClause = sql.substring( sql.indexOf("WHERE "), sql.length()); 
		assertEquals( whereClause, "WHERE (T1.strParam = ? AND T1.intParam > ? AND T2.p5 = T3.p6 AND (T3.p3 = ? OR T2.p4 IS NOT NULL))");				  
	}
		
	
	@Test
	public void test_12_sql(){
		
		//Prepare data		
		TableInfo tableInfo = TableInfo.Single("table1");		
	
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		String sql = selectHandler.Distinct().From(tableInfo).sql();
				
		//Check result
		assertEquals(sql, "SELECT DISTINCT * FROM table1 a");
	}	
	
	@Test
	public void test_12_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
				
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint("firstname", CompareType.LIKE,"firstname_1"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.From( tableInfo)
									 .Where( constraintContainer).execute();
		
		String lastname = result.getRowList().get(0).getColumn("lastname");
		
		//Check result
		assertEquals(lastname, "lastname_1");
		
	}
	
	@Test
	public void test_13_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
		
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("lastname"));
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint("firstname", CompareType.LIKE,"firstname_1"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo)
									 .Where( constraintContainer).execute();
		
		String lastname = result.getRowList().get(0).getColumn("lastname");
		
		//Check result
		assertEquals(lastname, "lastname_1");
		
	}
	
	@Test
	public void test_14_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
		
		Container constraintContainer = Container.And();	
		constraintContainer.add( Parameter.Constraint("gpa", CompareType.IN, 2.1, 2.2, 2.3));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.From(tableInfo)
									 .Where( constraintContainer).execute();
		
		String lastname1 = result.getRowList().get(0).getColumn("lastname");
		String lastname2 = result.getRowList().get(1).getColumn("lastname");
		String lastname3 = result.getRowList().get(2).getColumn("lastname");
		
		//Check result
		assertEquals(lastname1, "lastname_1");
		assertEquals(lastname2, "lastname_2");
		assertEquals(lastname3, "lastname_3");		
	}
	
	@Test
	public void test_15_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
		
		Container constraintContainer = Container.And();	
		constraintContainer.add( Parameter.Constraint("gpa", CompareType.GREQ, 2.5));
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByAsc("gpa"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.From( tableInfo)
									 .Where( constraintContainer)
									 .OrderBy( orderByParameterList).execute();
		
		String firstname1 = result.getRowList().get(0).getColumn("firstname");
		String firstname2 = result.getRowList().get(1).getColumn("firstname");
		String firstname3 = result.getRowList().get(2).getColumn("firstname");
		String firstname4 = result.getRowList().get(3).getColumn("firstname");
		
		//Check result
		assertEquals(firstname1, "firstname_5");
		assertEquals(firstname2, "firstname_6");
		assertEquals(firstname3, "firstname_7");	
		assertEquals(firstname4, "firstname_8");	
	}
	
	@Test
	public void test_16_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("student", "s");
		tableInfo.add("grade", "g");
		
		Container constraintContainer = Container.And();	
		constraintContainer.add( Parameter.Constraint( "s", "firstname", CompareType.LIKE, "%_1"));
		constraintContainer.add( Join.Equals("s", "id", "g", "student_id"));
				
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.From( tableInfo)
									 .Where( constraintContainer).execute();
		
		String grade1 = result.getRowList().get(0).getColumn("grade");
		String grade2 = result.getRowList().get(1).getColumn("grade");
		String grade3 = result.getRowList().get(2).getColumn("grade");
		String grade4 = result.getRowList().get(3).getColumn("grade");
		
		//Check result
		assertEquals( grade1, "A");
		assertEquals( grade2, "A-");			
		assertEquals( grade3, "B+");
		assertEquals( grade4, "B");
	}
	
	@Test
	public void test_17_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("student", "s");
		tableInfo.add("grade", "g");
		tableInfo.add("course", "c");
		
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("s", "firstname"));
		
		Container constraintContainer = Container.And();		
		constraintContainer.add( Parameter.Constraint( "c", "name", CompareType.LIKE, "%_1"));
		constraintContainer.add( Join.Equals("c", "id", "g", "course_id"));
		constraintContainer.add( Join.Equals("g", "student_id", "s", "id"));
				
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByAsc( "s", "firstname"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo)
									 .Where( constraintContainer)
									 .OrderBy( orderByParameterList).execute();
		
		String firstname1 = result.getRowList().get(0).getColumn("firstname");
		String firstname2 = result.getRowList().get(1).getColumn("firstname");
		
		//Check result
		assertEquals( firstname1, "firstname_1");
		assertEquals( firstname2, "firstname_2");		
	}
	
	@Test
	public void test_18_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Multi();
		tableInfo.add("student", "s");
		tableInfo.add("grade", "g");
		tableInfo.add("course", "c");
		
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("s", "firstname"));
		
		Container constraintContainer = Container.And();		
		constraintContainer.add( Parameter.Constraint( "c", "name", CompareType.LIKE, "course_1"));
		constraintContainer.add( Join.Equals("c", "id", "g", "course_id"));
		constraintContainer.add( Join.Equals("g", "student_id", "s", "id"));
		
		Container subConstraintContainer = Container.And();
		subConstraintContainer.add( Parameter.Constraint( "s", "birthdate", CompareType.SM, new Date( 80, 1, 3)));
		subConstraintContainer.add( Parameter.Constraint( "s", "gpa", CompareType.GR, 2.1));
		
		constraintContainer.add( subConstraintContainer);
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByAsc( "s", "firstname"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo)
									 .Where( constraintContainer)
									 .OrderBy( orderByParameterList).execute();
		
		String firstname = result.getRowList().get(0).getColumn("firstname");
				
		//Check result
		assertEquals( firstname, "firstname_2");					
	}
	
	@Test
	public void test_19_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("firstname"));
		
		Container constraintContainer = Container.And();;		
		constraintContainer.add( Parameter.Constraint( "lastname", CompareType.NOT_LIKE, "%_1"));
		constraintContainer.add( Parameter.Constraint( "lastname", CompareType.NOT_LIKE, "%_2"));
		constraintContainer.add( Parameter.Constraint( "lastname", CompareType.NOT_LIKE, "%_3"));
		
		
		Container subConstraintContainer = Container.Or();
		subConstraintContainer.add( Parameter.Constraint( "gpa", CompareType.SMEQ, 2.4));
		subConstraintContainer.add( Parameter.Constraint( "gpa", CompareType.GR, 2.1));
		
		constraintContainer.add( subConstraintContainer);
		
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByAsc( "firstname"));
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo)
									 .Where( constraintContainer)
									 .OrderBy( orderByParameterList).execute();
		
		String firstname = result.getRowList().get(0).getColumn("firstname");
				
		//Check result
		assertEquals( firstname, "firstname_4");					
	}
	
	@Test
	public void test_20_exec(){
						
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("grade");
				
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("student_id"));
		selectParameterList.add( Parameter.SelectFunction( FunctionType.COUNT, "course_id"));
		selectParameterList.add( Parameter.SelectFunction( FunctionType.MIN, "grade"));
					
		GroupByParameterList groupByParameterList = ParameterList.GroupBy();
		groupByParameterList.add( Parameter.GroupByAsc( "student_id"));
		
		Container havingContainer = Container.And();
		havingContainer.add( Parameter.ConstraintFunction( FunctionType.COUNT, "course_id", CompareType.GR, 3));
		
		
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo)
									 .GroupBy( groupByParameterList)
									 .Having( havingContainer).execute();
		
		//Check result	
		Row row1 = result.getRowList().get(0);
		Row row2 = result.getRowList().get(1);
		
		int student_id_1 = row1.getColumn("student_id");
		int student_id_2 = row2.getColumn("student_id");
		long countCourseID = row1.getColumn("count(a.course_id)");	//count returns long!
		String grade = row1.getColumn("min(a.grade)");
		
		assertEquals( student_id_1, 1);
		assertEquals( student_id_2, 2);
		assertEquals( countCourseID, 4);
		assertEquals( grade, "A");
				
	}
	
	@Test
	public void test_21_exec(){
		
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("grade");
			
		OrderByParameterList orderByParameterList = ParameterList.OrderBy();
		orderByParameterList.add( Parameter.OrderByAsc("student_id"));
			
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.From( tableInfo)
									 .OrderBy( orderByParameterList)
									 .Limit( 5, 2).execute();
		
		//Check result	
		Row row1 = result.getRowList().get(0);
		Row row2 = result.getRowList().get(1);
		
		String grade1 = row1.getColumn("grade");
		String grade2 = row2.getColumn("grade");
		
		assertEquals( grade1, "B+");
		assertEquals( grade2, "A-");				
	}
	
	@Test
	public void test_22_exec(){
				
		//Prepare data
		TableInfo tableInfo = TableInfo.Single("student");
			
		SelectParameterList selectParameterList = ParameterList.Select();
		selectParameterList.add( Parameter.Select("id").As("student_id"))
						   .add( Parameter.SelectFunction( FunctionType.MIN, "gpa").As( "note"));
		
		Container constraintContainer = Container.And();
		constraintContainer.add( Parameter.Constraint("firstname", CompareType.LIKE, "%_1"));
			
		//Perform action
		MySQLSelectHandler selectHandler = new MySQLSelectHandler( cushyDBConnection);
		Result result = selectHandler.Select( selectParameterList)
									 .From( tableInfo).execute();
		
		//Check result	
		Row row = result.getRowList().get(0);		
		int student_id = row.getColumn("student_id");
		double note = row.getColumn("note");
		
		assertEquals( student_id, 1);
		assertEquals( note, 2.1, 0);				
	}
		
}
