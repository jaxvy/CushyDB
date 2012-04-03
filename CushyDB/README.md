#CushyDB

CushyDB is a JDBC wrapper API for writing type-safe SQL like queries. 
It encourages contructing structured SQL queries in a readable way. 
You can check the code below and the unit tests within the project to learn how to use CushyDB.


##Usage

Example 1 shows how the perform a simple query on a table named "student" with columns "id","firstname", "lastname", "birthdate" and "gpa".


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
