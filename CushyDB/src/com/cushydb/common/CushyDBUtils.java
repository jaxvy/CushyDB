package com.cushydb.common;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.xml.DOMConfigurator;

import com.cushydb.bean.Parameter.ConstraintParameter;
import com.cushydb.bean.Parameter.InsertParameter;
import com.cushydb.bean.Parameter.ParameterBase;
import com.cushydb.bean.ParameterInterface;


public class CushyDBUtils {

	private CushyDBUtils(){}
	
	public static void initLoj4J( ){
		
		DOMConfigurator.configureAndWatch("config/log4j_config.xml");
	}
			
	public static String generateFromUnit( String tableName, String tableAlias){
		
		return tableName + " " + tableAlias;
	}
	
	public static void bindWithObjectArray( PreparedStatement preparedStatement, Object[] parameterValueInSequenceList){
		
		try{
			int index = 1;
			for( Object parameterValue: parameterValueInSequenceList){
				
				bind( preparedStatement, index, parameterValue);											
				index++;
			}		
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while binding parameters to PreparedStatement", e);
		}
	}
	
	public static void bindWithObjectList( PreparedStatement preparedStatement, List<Object> parameterValueInSequenceList){
		
		try{
			int index = 1;
			for( Object parameterValue: parameterValueInSequenceList){
				
				bind( preparedStatement, index, parameterValue);											
				index++;
			}		
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while binding parameters to PreparedStatement", e);
		}
	}
	
	public static void bindWithInsertParameterList( PreparedStatement preparedStatement, List<ParameterInterface> parameterList){
		
		try{
			int index = 1;
			for( ParameterInterface pi: parameterList){
							
				InsertParameter p = (InsertParameter)pi;
				
				bind( preparedStatement, index, p.getParameterValue());											
				index++;
			}		
		}
		catch( SQLException e){
			throw new CushyDBException("An exception has occurred while binding parameters to PreparedStatement", e);
		}
	}
	
	private static void bind( PreparedStatement preparedStatement, int index, Object parameterValue) throws SQLException{
		
		if( parameterValue instanceof Date){
			 preparedStatement.setDate( index, (Date)parameterValue);
		}
		else if( parameterValue instanceof Double){
			preparedStatement.setDouble( index, (Double)parameterValue);
		}
		else if( parameterValue instanceof Integer){
			preparedStatement.setInt( index, (Integer)parameterValue);	
		}
		else if( parameterValue instanceof Long){
			preparedStatement.setLong( index, (Long) parameterValue);
		}
		else if( parameterValue instanceof String){
			preparedStatement.setString( index, (String) parameterValue);
		}
		else if( parameterValue instanceof Timestamp){
			preparedStatement.setTimestamp( index, (Timestamp) parameterValue);	
		}
		else if( parameterValue instanceof Clob){
			preparedStatement.setClob( index, (Clob) parameterValue);
		}
		else if( parameterValue instanceof Blob){
			preparedStatement.setBlob( index, (Blob) parameterValue);
		}
		else if( parameterValue instanceof Float){
			preparedStatement.setFloat( index, (Float) parameterValue);
		}
		else if( parameterValue instanceof byte[]){
			preparedStatement.setBytes( index, (byte[]) parameterValue);
		}
		else if( parameterValue instanceof Short){
			preparedStatement.setShort( index, (Short) parameterValue);
		}
		else if( parameterValue instanceof Boolean){
			preparedStatement.setBoolean( index, (Boolean)parameterValue);
		}
		else{
			preparedStatement.setObject( index, parameterValue);
		}
	}	
}
