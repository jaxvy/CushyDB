package com.cushydb.bean;

import com.cushydb.common.CushyDBValidator;
import com.cushydb.enums.CompareType;
import com.cushydb.enums.FunctionType;

public class Parameter {

	public static final String SINGLE_TABLE_ALIAS = "a";
	
	
	public static abstract class ParameterBase {
		
		//Properties
		private String tableAlias;
		private String parameterName;
		
		//Constructor
		protected ParameterBase( String tableAlias, String parameterName) {
			
			CushyDBValidator.isInputNotNullAndNonEmptyOrThrow( tableAlias, "TableAlias cannot be null or empty");
			CushyDBValidator.isInputNotNullAndNonEmptyOrThrow( parameterName, "ParameterName cannot be null or empty");
			
			this.tableAlias = tableAlias;
			this.parameterName = parameterName;
		}
					
		//Getters
		public String getTableAlias() {
			return tableAlias;
		}
		public String getParameterName() {
			return parameterName;
		}		
	}
	
	
	public static class ConstraintParameter extends ParameterBase implements ParameterInterface{

		//Properties
		private CompareType compareType;
		private Object parameterValue = null;
		
		//Constructors
		private ConstraintParameter( String parameterName, CompareType compareType, Object parameterValue){
			super( SINGLE_TABLE_ALIAS, parameterName);		
			this.compareType = compareType;
			this.parameterValue = parameterValue;
		}	
		
		private ConstraintParameter( String parameterName, CompareType compareType, Object... parameterValueArray){
			super( SINGLE_TABLE_ALIAS, parameterName);	
			this.compareType = compareType;
			this.parameterValue = parameterValueArray;
		}	
		
		private ConstraintParameter( String tableAlias, String parameterName, CompareType compareType, Object parameterValue){
			super( tableAlias, parameterName);		
			this.compareType = compareType;
			this.parameterValue = parameterValue;
		}	
		
		private ConstraintParameter( String tableAlias, String parameterName, CompareType compareType, Object... parameterValueArray){
			super( tableAlias, parameterName);	
			this.compareType = compareType;
			this.parameterValue = parameterValueArray;
		}

		//Getters
		public CompareType getCompareType() {
			return compareType;
		}
		public Object getParameterValue() {
			return parameterValue;
		}					
	}
	
	public static class ConstraintFunctionParameter extends ParameterBase implements ParameterInterface{

		//Properties
		private FunctionType functionType;
		private CompareType compareType;
		private Object parameterValue;
		
		//Constructors	
		private ConstraintFunctionParameter( FunctionType functionType, String parameterName, CompareType compareType, Object parameterValue) {
			super( SINGLE_TABLE_ALIAS, parameterName);			
			this.functionType = functionType;
			this.compareType = compareType;
			this.parameterValue = parameterValue;
		}	
		
		private ConstraintFunctionParameter( FunctionType functionType, String tableAlias, String parameterName, CompareType compareType, Object parameterValue) {
			super( tableAlias, parameterName);			
			this.functionType = functionType;
			this.compareType = compareType;
			this.parameterValue = parameterValue;
		}
		
		//Getters
		public FunctionType getFunctionType() {
			return functionType;
		}
		public CompareType getCompareType() {
			return compareType;
		}
		public Object getParameterValue() {
			return parameterValue;
		}		
	}
	
	public static class SelectParameter extends ParameterBase implements ParameterInterface{

		//Properties
		private String parameterNameAlias;
		
		//Constructors
		private SelectParameter( String parameterName){
			super( SINGLE_TABLE_ALIAS, parameterName);	
			parameterNameAlias = null;
		}			
		
		private SelectParameter( String tableAlias, String parameterName){
			super( tableAlias, parameterName);
			parameterNameAlias = null;
		}			
		
		//Methods
		public SelectParameter As( String parameterNameAlias){
			this.parameterNameAlias = parameterNameAlias;
			return this;
		}
		
		public String getParameterNameAlias(){
			return parameterNameAlias;
		}
	}
	
	public static class SelectFunctionParameter extends ParameterBase implements ParameterInterface{

		//Property
		private FunctionType functionType;
		private String parameterNameAlias;
				
		//Constructors
		private SelectFunctionParameter( FunctionType functionType, String parameterName) {
			super( SINGLE_TABLE_ALIAS, parameterName);			
			this.functionType = functionType;
		}	
		
		private SelectFunctionParameter( FunctionType functionType, String tableAlias, String parameterName) {
			super( tableAlias, parameterName);			
			this.functionType = functionType;
		}
		
		//Getter
		public FunctionType getFunctionType() {
			return functionType;
		}		
		
		public SelectFunctionParameter As( String parameterNameAlias){
			this.parameterNameAlias = parameterNameAlias;
			return this;
		}
		
		public String getParameterNameAlias(){
			return parameterNameAlias;
		}
	}
	
	public static class SetParameter extends ParameterBase implements ParameterInterface{

		//Property
		private Object parameterValue;
		
		//Constructors
		private SetParameter( String parameterName, Object parameterValue){
			super( SINGLE_TABLE_ALIAS, parameterName);		
			this.parameterValue = parameterValue;
		}								
		
		private SetParameter( String tableAlias, String parameterName, Object parameterValue){
			super( tableAlias, parameterName);		
			this.parameterValue = parameterValue;
		}

		//Getter
		public Object getParameterValue() {
			return parameterValue;
		}
	}
	
	public static class InsertParameter extends ParameterBase implements ParameterInterface{

		//Property
		private Object parameterValue;
		
		//Constructors
		private InsertParameter( String parameterName, Object parameterValue){
			super( SINGLE_TABLE_ALIAS, parameterName);		
			this.parameterValue = parameterValue;
		}								
		
		private InsertParameter( String tableAlias, String parameterName, Object parameterValue){
			super( tableAlias, parameterName);		
			this.parameterValue = parameterValue;
		}

		//Getter
		public Object getParameterValue() {
			return parameterValue;
		}
	}
	
	public static class OrderByParameter extends ParameterBase implements ParameterInterface{
		
		//Property
		private boolean isAscending;
		
		//Constructors
		private OrderByParameter( String parameterName, boolean isAscending){
			super( SINGLE_TABLE_ALIAS, parameterName);
			this.isAscending = isAscending;
		}
		
		private OrderByParameter( String tableAlias, String parameterName, boolean isAscending){
			super( tableAlias, parameterName);
			this.isAscending = isAscending;
		}

		//Getter
		public boolean isAscending() {
			return isAscending;
		}	
	}
	
	public static class GroupByParameter extends ParameterBase implements ParameterInterface{
		
		//Property
		private boolean isAscending;
		
		//Constructors
		private GroupByParameter( String parameterName, boolean isAscending){
			super( SINGLE_TABLE_ALIAS, parameterName);
			this.isAscending = isAscending;
		}
		
		private GroupByParameter( String tableAlias, String parameterName, boolean isAscending){
			super( tableAlias, parameterName);
			this.isAscending = isAscending;
		}

		//Getter
		public boolean isAscending() {
			return isAscending;
		}	
	}
						
		
	//Static factories
	public static ConstraintParameter Constraint( String parameterName, CompareType compareType, Object parameterValue){		
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValue);
		return new ConstraintParameter( parameterName, compareType, parameterValue);				
	}
	
	public static ConstraintParameter Constraint( String tableAlias, String parameterName, CompareType compareType, Object parameterValue){
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValue);
		return new ConstraintParameter( tableAlias, parameterName, compareType, parameterValue);		
	}
	
	public static ConstraintParameter Constraint( String parameterName, CompareType compareType, Object... parameterValueArray){	
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValueArray);
		return new ConstraintParameter( parameterName, compareType, parameterValueArray);
	}
	
	public static ConstraintParameter Constraint( String tableAlias, String parameterName, CompareType compareType, Object... parameterValueArray){	
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValueArray);
		return new ConstraintParameter( tableAlias, parameterName, compareType, parameterValueArray);
	}

	public static ConstraintFunctionParameter ConstraintFunction( FunctionType functionType, String parameterName, CompareType compareType, Object parameterValue){
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValue);
		CushyDBValidator.isConstraintFunctionParametersValidOrThrow( functionType, parameterValue);
		return new ConstraintFunctionParameter( functionType, parameterName, compareType, parameterValue);
	}
	
	public static ConstraintFunctionParameter ConstraintFunction( FunctionType functionType, String tableAlias, String parameterName, CompareType compareType, Object parameterValue){
		CushyDBValidator.isConstraintParametersValidOrThrow( compareType, parameterValue);
		CushyDBValidator.isConstraintFunctionParametersValidOrThrow( functionType, parameterValue);
		return new ConstraintFunctionParameter( functionType, tableAlias, parameterName, compareType, parameterValue);	
	}
	
	public static SelectParameter Select( String parameterName){		
		return new SelectParameter( parameterName);
	}
	
	public static SelectParameter Select( String tableAlias, String parameterName){		
		return new SelectParameter( tableAlias, parameterName);
	}
	
	public static SelectParameter Constraint( String tableAlias, String parameterName){		
		return new SelectParameter( tableAlias, parameterName);	
	}
	
	public static SelectFunctionParameter SelectFunction( FunctionType functionType, String parameterName){
		CushyDBValidator.isInputNotNullOrThrow( functionType, "FunctionType cannot be null");
		return new SelectFunctionParameter( functionType, parameterName);
	}
	
	public static SelectFunctionParameter ConstraintFunction( FunctionType functionType, String tableAlias, String parameterName){	
		CushyDBValidator.isInputNotNullOrThrow( functionType, "FunctionType cannot be null");
		return new SelectFunctionParameter( functionType, tableAlias, parameterName);
	}
	
	public static SetParameter Set( String parameterName, Object parameterValue){	
		CushyDBValidator.isParameterValueValidOrThrow( parameterValue, "ParameterValue only be of types: " + CushyDBValidator.ALLOWED_TYPES);				
		return new SetParameter( parameterName, parameterValue);
	}
	
	public static SetParameter Set( String tableAlias, String parameterName, Object parameterValue){
		CushyDBValidator.isParameterValueValidOrThrow( parameterValue, "ParameterValue only be of types: " + CushyDBValidator.ALLOWED_TYPES);
		return new SetParameter( tableAlias, parameterName, parameterValue);
	}
	
	public static InsertParameter Insert( String parameterName, Object parameterValue){		
		CushyDBValidator.isParameterValueValidOrThrow( parameterValue, "ParameterValue only be of types: " + CushyDBValidator.ALLOWED_TYPES);
		return new InsertParameter( parameterName, parameterValue);
	}
	
	public static InsertParameter Insert( String tableAlias, String parameterName, Object parameterValue){	
		CushyDBValidator.isParameterValueValidOrThrow( parameterValue, "ParameterValue only be of types: " + CushyDBValidator.ALLOWED_TYPES);
		return new InsertParameter( tableAlias, parameterName, parameterValue);
	}
	
	public static OrderByParameter OrderByAsc( String parameterName){		
		return new OrderByParameter( parameterName, true);
	}
	
	public static OrderByParameter OrderByDesc( String parameterName){		
		return new OrderByParameter( parameterName, false);
	}
	
	public static OrderByParameter OrderByAsc( String tableAlias, String parameterName){		
		return new OrderByParameter( tableAlias, parameterName, true);
	}
	
	public static OrderByParameter OrderByDesc( String tableAlias, String parameterName){		
		return new OrderByParameter( tableAlias, parameterName, false);
	}
	
	public static GroupByParameter GroupByAsc( String parameterName){		
		return new GroupByParameter( parameterName, true);
	}
	
	public static GroupByParameter GroupByAsc( String tableAlias, String parameterName){		
		return new GroupByParameter( tableAlias, parameterName, true);
	}
	
	public static GroupByParameter GroupByDesc( String parameterName){		
		return new GroupByParameter( parameterName, false);
	}
	
	public static GroupByParameter GroupByDecs( String tableAlias, String parameterName){		
		return new GroupByParameter( tableAlias, parameterName, false);
	}	
}
