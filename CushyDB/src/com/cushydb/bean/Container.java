package com.cushydb.bean;

import java.util.ArrayList;
import java.util.List;

import com.cushydb.bean.Parameter.ConstraintFunctionParameter;
import com.cushydb.bean.Parameter.ConstraintParameter;
import com.cushydb.enums.CompareType;
import com.cushydb.enums.ContainerType;
import com.cushydb.enums.FunctionType;

public class Container {

	//Properties
	private List<ParameterInterface> parameterList;
	private List<Join> joinList;
	private List<Container> containerList;
	
	private ContainerType containerType;
	
	//Constructor
	private Container( ContainerType containerType){
		this.containerType = containerType;
		
		parameterList = new ArrayList<ParameterInterface>();
		joinList = new ArrayList<Join>();
		containerList = new ArrayList<Container>();
	}
		
	//Static factories
	public static Container And(){
		
		return new Container( ContainerType.AND);		
	}
	
	public static Container Or(){
		
		return new Container( ContainerType.OR);		
	}
	
	
	//Methods
	public Container add( ConstraintParameter parameter){
		
		parameterList.add( parameter);
		return this;
	}
	
	public Container add( String parameterName, CompareType compareType, Object parameterValue){
		parameterList.add( Parameter.Constraint( parameterName, compareType, parameterValue));
		return this;
	}	
	
	public Container add( String parameterName, CompareType compareType, Object... parameterValueArray){
		parameterList.add( Parameter.Constraint( parameterName, compareType, parameterValueArray));
		return this;
	}	
	
	public Container add( String tableAlias, String parameterName, CompareType compareType, Object parameterValue){
		parameterList.add( Parameter.Constraint( tableAlias, parameterName, compareType, parameterValue));
		return this;
	}	
	
	public Container add( String tableAlias, String parameterName, CompareType compareType, Object... parameterValueArray){
		parameterList.add( Parameter.Constraint( tableAlias, parameterName, compareType, parameterValueArray));
		return this;
	}
	
	public Container add( ConstraintFunctionParameter parameter){
		
		parameterList.add( parameter);
		return this;
	}
	
	public Container add( FunctionType functionType, String parameterName, CompareType compareType, Object parameterValue){
		parameterList.add( Parameter.ConstraintFunction(functionType, parameterName, compareType, parameterValue));
		return this;
	}	
	
	public Container add( FunctionType functionType, String parameterName, CompareType compareType, Object... parameterValueArray){
		parameterList.add( Parameter.ConstraintFunction(functionType, parameterName, compareType, parameterValueArray));
		return this;
	}	
	
	public Container add( FunctionType functionType, String tableAlias, String parameterName, CompareType compareType, Object parameterValue){
		parameterList.add( Parameter.ConstraintFunction( functionType, tableAlias, parameterName, compareType, parameterValue));
		return this;
	}	
	
	public Container add( FunctionType functionType, String tableAlias, String parameterName, CompareType compareType, Object... parameterValueArray){
		parameterList.add( Parameter.ConstraintFunction( functionType, tableAlias, parameterName, compareType, parameterValueArray));
		return this;
	}
	
	
	public Container add( Join join){
		
		joinList.add( join);
		return this;
	}
	
	public Container add( Container container){
		
		containerList.add( container);
		return this;
	}

	public List<ParameterInterface> getParameterList(){
		return parameterList;
	}
	
	public List<Join> getJoinList(){
		return joinList;
	}

	public List<Container> getContainerList(){
		return containerList;
	}

	public ContainerType getContainerType(){
		return containerType;
	}
}
