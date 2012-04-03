package com.cushydb.enums;

public enum ContainerType {

	AND("AND"),
	OR("OR");
	
	
	//Properties
	private String value;
	
	private ContainerType( String value){
		this.value = value;
	}
	
	public String toString(){
		return value;
	}
}
