package com.cushydb.enums;

public enum CompareType {

	//MYSQL compare types: http://dev.mysql.com/doc/refman/5.0/en/comparison-operators.html
	EQ("="), 
	NEQ("<>"),
	GR(">"),
	GREQ(">="),
	SM("<"),
	SMEQ("<="),
	LIKE("LIKE"),
	NOT_LIKE("NOT LIKE"),
	IS("IS"),
	IS_NOT("IS NOT"),
	IN("IN"),
	NOT_IN("NOT IN");
	
	//Properties
	private String jdbcValue;
	
	private CompareType( String jdbcValue){
		this.jdbcValue = jdbcValue;
	}

	public String getJDBCValue(){
		return jdbcValue;
	}	
}
