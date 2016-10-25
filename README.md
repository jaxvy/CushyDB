#CushyDB

CushyDB is a JDBC wrapper API for easily constructing dynamic SQL queries. 
The structure of the API allows to write readable and flexible code for SQL operations.
CushyDB embraces SQL structure in it's API design and hides the cumbersome details of the JDBC API.
It should be noted that CushyDB is not an ORM. 
Currently CushyDB is tested only with MySQL (5.1.10) however support for other databases will be added.

You can check the examples below and the unit tests within the project to learn how to use CushyDB.


##Examples

The examples assume that we are working on 3 tables defined as below:
CREATE TABLE STUDENT (ID INT NOT NULL AUTO_INCREMENT, FIRSTNAME VARCHAR(64), LASTNAME VARCHAR(64), BIRTHDATE DATETIME, GPA DOUBLE, PRIMARY KEY (ID))
CREATE TABLE COURSE (ID INT NOT NULL AUTO_INCREMENT, NAME VARCHAR(64), PRIMARY KEY (ID))
CREATE TABLE GRADE (STUDENT_ID INT, COURSE_ID INT, GRADE VARCHAR(2), CONSTRAINT FK_STUDENT FOREIGN KEY ( STUDENT_ID ) REFERENCES STUDENT (ID ), CONSTRAINT FK_COURSE FOREIGN KEY ( COURSE_ID ) REFERENCES COURSE (ID ))


Example 1 shows how the perform a simple query 

	TableInfo tableInfo = TableInfo.Single("student");
	
	SelectParameterList selectParameterList = ParameterList.Select();
	selectParameterList.add("lastname");
	
	Container constraintContainer = Container.And();
	constraintContainer.add("firstname", CompareType.LIKE, "firstname_1");
	
	MySQLSelectHandler selectHandler = new MySQLSelectHandler(cushyDBConnection);
	Result result = selectHandler.Select(selectParameterList)
								 .From(tableInfo)
								 .Where(constraintContainer).execute();
	
	String lastname = result.getRowList().get(0).getColumn("lastname");

	
Example 2 shows how to perform a more complex query

	TableInfo tableInfo = TableInfo.Single("student");
				
	SelectParameterList selectParameterList = ParameterList.Select();
	selectParameterList.add("firstname");
	
	Container constraintContainer = Container.And();		
	constraintContainer.add("lastname", CompareType.NOT_LIKE, "%_1");
	constraintContainer.add("lastname", CompareType.NOT_LIKE, "%_2");
	constraintContainer.add("lastname", CompareType.NOT_LIKE, "%_3");
		
	Container subConstraintContainer = Container.Or();
	subConstraintContainer.add("gpa", CompareType.SMEQ, 2.4);
	subConstraintContainer.add("gpa", CompareType.GR, 2.1);
	
	constraintContainer.add(subConstraintContainer);
	
	OrderByParameterList orderByParameterList = ParameterList.OrderBy();
	orderByParameterList.addAsc("firstname");
	
	MySQLSelectHandler selectHandler = new MySQLSelectHandler(cushyDBConnection);
	Result result = selectHandler.Select( selectParameterList)
								 .From(tableInfo)
								 .Where(constraintContainer)
								 .OrderBy(orderByParameterList).execute();
								 
Example 3 shows how to perform a query with join operations and table aliases (note that parameters can be also be added with the Parameter factory pattern)

	TableInfo tableInfo = TableInfo.Multi();
	tableInfo.add("student", "s");
	tableInfo.add("grade", "g");
	
	Container constraintContainer = Container.And();	
	constraintContainer.add( Parameter.Constraint("s", "firstname", CompareType.LIKE, "%_1"));
	constraintContainer.add( Join.Equals("s", "id", "g", "student_id"));
		
	MySQLSelectHandler selectHandler = new MySQLSelectHandler(cushyDBConnection);
	Result result = selectHandler.From(tableInfo)
								 .Where(constraintContainer).execute();

Example 4 shows how to perform a simple update operation

	TableInfo tableInfo = TableInfo.Single("student");
		
	SetParameterList setParameterList = ParameterList.Set();		
	setParameterList.add( Parameter.Set("firstname", "updated_firstname_1"));
	
	Container constraintContainer = Container.And();
	constraintContainer.add( Parameter.Constraint("firstname", CompareType.LIKE, "firstname_1"));
	
	MySQLUpdateHandler updateHandler = new MySQLUpdateHandler(cushyDBConnection);
	updateHandler.Update(tableInfo)
				 .Set(setParameterList)
				 .Where(constraintContainer).execute();
				 
Example 5 shows how to perform a simple delete operation

	TableInfo tableInfo = TableInfo.Single("grade");
		
	Container constraintContainer = Container.And();
	constraintContainer.add( Parameter.Constraint("grade", CompareType.LIKE, "A"));
	// constraintContainer.add("grade", CompareType.LIKE, "A"); can also be used like this, your choice
	
	MySQLDeleteHandler deleteHandler = new MySQLDeleteHandler( cushyDBConnection);
	deleteHandler.Delete()
				 .From(tableInfo)
				 .Where(constraintContainer).execute();
				 
Example 6 shows how to perform a simple insert operation

	String tableName = "course";
	
	InsertParameterList insertParameterList = ParameterList.Insert();
	insertParameterList.add("name", "Programming");
							
	//Perform action
	MySQLInsertHandler insertHandler = new MySQLInsertHandler( cushyDBConnection);		
	Result result = insertHandler.Insert(insertParameterList)
								 .Into(tableName)
								 .ReturnKey().execute();

Example 7 shows how to perform batch insert	
		
	String tableName = "course";
				
	InsertParameterList baseInsertParameterList = ParameterList.Insert();
	baseInsertParameterList.add("name", "Programming_0")
					
	MySQLInsertHandler insertHandler = new MySQLInsertHandler(cushyDBConnection);		
	insertHandler.Insert(baseInsertParameterList)
				 .Into(tableName)
				 .ReturnKey();
	
	
	for( int i = 1; i <= 9; i++){
		Object[] batchInsertParameters = {"Programming_" + i};
		insertHandler.AddBatch(batchInsertParameters);				
	}
	
	//Perform action
	insertHandler.BatchSize(100)	//BatchSize is optional
				 .execute();		//Batch insert does not provide returning of generated keys
		
##Usage

In order to use CushyDB in your project, make sure you have added cushydb-1.0.jar, mysql-connector-java-5.1.10-bin.jar (or later), junit-4.9.jar (or later) and log4j-1.2.16.jar (or later) in your classpath.
You can download cushydb-1.0.jar from downloads section or find it under dist folder or you can generate it using src folder of the project.

Have fun.
		
