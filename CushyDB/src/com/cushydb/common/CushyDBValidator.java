package com.cushydb.common;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import com.cushydb.bean.Parameter.InsertParameter;
import com.cushydb.bean.ParameterInterface;
import com.cushydb.bean.ParameterList.InsertParameterList;
import com.cushydb.enums.CompareType;
import com.cushydb.enums.FunctionType;

public class CushyDBValidator {

	private CushyDBValidator(){}
	
	public static final String ALLOWED_TYPES = "[String,Integer,Double,Float,Long,Date,Clob,Blob,byte[],Short,Boolean,Timestamp]";
	
	public static void hasUniqueParameterNames( InsertParameterList parameterList){
			
		Set<String> parameterNameSet = new HashSet<String>();
		for( ParameterInterface pi: parameterList.getList()){
			
			InsertParameter p = (InsertParameter)pi;
			
			if( !parameterNameSet.contains( p.getTableAlias() + " " + p.getParameterName())){
				parameterNameSet.add( p.getTableAlias() + " " + p.getParameterName());
			}
			else{
				throw new CushyDBException("hasUniqueParameterNames exception for parameterList: " + parameterList.toString());
			}
		}				
	}	
	
	public static void isConstraintParametersValidOrThrow( CompareType compareType, Object parameterValue){
				
		if( compareType == CompareType.IN || compareType == CompareType.NOT_IN){
			
			if( !(parameterValue instanceof Object[])){
				throw new CushyDBException("CompareType " + compareType.getJDBCValue() + " can only be used with parameterValueList");
			}			
			else{
				
				for( Object element: (Object[])parameterValue){
					
					isParameterValueValidOrThrow( element, "CompareType " + compareType.getJDBCValue() + " can only be used with types: " + ALLOWED_TYPES );
				}
			}		
		}
		else{	
			
			isParameterValueValidOrThrow( parameterValue, "CompareType " + compareType.getJDBCValue() +	" can only be used with types: " + ALLOWED_TYPES);				
		}
		
		if( compareType == CompareType.LIKE || compareType == CompareType.NOT_LIKE){
			
			if( !(parameterValue instanceof String)){
				throw new CushyDBException( "CompareType " + compareType + " can only be used with type: [String]");
			}
		}
	}
	
	public static void isConstraintFunctionParametersValidOrThrow( FunctionType functionType, Object parameterValue){
		
		if( functionType == null){
			throw new CushyDBException( "FunctionType cannot be null");
		}
		
		isParameterValueValidOrThrow( parameterValue, "FunctionType " + functionType + "can only be used with types: " + ALLOWED_TYPES );			
	}
	
	public static void isInputNotNullAndNonEmptyOrThrow( String input, String errorMessage){
		if( input == null || input.trim().isEmpty()){
			throw new CushyDBException( errorMessage);
		}
	}
	
	public static void isInputNotNullOrThrow( Object input, String errorMessage){
		if( input == null){
			throw new CushyDBException( errorMessage);
		}
	}
	
	public static void isParameterValueValidOrThrow( Object parameterValue, String message){
		
		if(!(parameterValue instanceof String 		||
			 parameterValue instanceof Integer 		||
			 parameterValue instanceof Double 		|| 
			 parameterValue instanceof Float 		||
			 parameterValue instanceof Long 		||
			 parameterValue instanceof Date 		||
			 parameterValue instanceof Clob 		||
			 parameterValue instanceof Blob 		||
			 parameterValue instanceof byte[] 		||
			 parameterValue instanceof Short 		||
			 parameterValue instanceof Boolean		||
			 parameterValue instanceof Timestamp	||
			 parameterValue == null)){
						
			throw new CushyDBException( message);		
		}	
	}
}
