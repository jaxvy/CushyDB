package com.cushydb.bean;

import com.cushydb.enums.CompareType;

public class Join{

	//Properties
	
	private String parameterName1;
	private String tableAlias1;
	
	private String parameterName2;
	private String tableAlias2;
	
	private CompareType compareType;
	
	//Constructor
	private Join(){}
	
	//Static factories
	public static Join Equals( String tableAlias1, String parameterName1, String tableAlias2, String parameterName2){
		
		Join j = new Join();
		j.tableAlias1 = tableAlias1;
		j.parameterName1 = parameterName1;
		j.tableAlias2 = tableAlias2;
		j.parameterName2 = parameterName2;
		j.compareType = CompareType.EQ;
		
		return j;
	}
	
	public static Join WithCompareType( String tableAlias1, String parameterName1, CompareType compareType, String tableAlias2, String parameterName2){
		
		Join j = new Join();
		j.tableAlias1 = tableAlias1;
		j.parameterName1 = parameterName1;
		j.tableAlias2 = tableAlias2;
		j.parameterName2 = parameterName2;
		j.compareType = compareType;
		
		return j;
	}
		
	//Getter and setter methods	

	public String getTableAlias1() {
		return tableAlias1;
	}
	
	public String getParameterName1() {
		return parameterName1;
	}


	public String getParameterName2() {
		return parameterName2;
	}

	public String getTableAlias2() {
		return tableAlias2;
	}

	public CompareType getCompareType() {
		return compareType;
	}	
	
	//Utility functions
	
	@Override
	public String toString(){
		return "[" + tableAlias1 + ", " + parameterName1 + ", " + tableAlias2 + ", " + parameterName2 + ", " + compareType.getJDBCValue()  + "]";
	}
	
	public String generateConstraintUnit(){
		
		String whereUnitStr = tableAlias1 + "." + parameterName1 + " " + compareType.getJDBCValue() + " " + tableAlias2 + "." + parameterName2;
		
		return whereUnitStr;
	}
}
