package com.cushydb.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CushyDBConnection {

	private Connection jdbcConnection;
	
	public CushyDBConnection(){}
	
	public void setJDBCConnection( Connection databaseConnection){
		jdbcConnection = databaseConnection;
	}
		
	public PreparedStatement prepareStatement( String sql) throws SQLException{
		return jdbcConnection.prepareStatement( sql, PreparedStatement.RETURN_GENERATED_KEYS);
	}
		
	public void commitTransaction(){
		try {
			jdbcConnection.commit();
		}
		catch( SQLException e) {	
			throw new CushyDBException( "An exception occurred while committing transaction", e);
		}
	}	
	
	public void rollbackTransaction(){
		try {
			jdbcConnection.rollback();
		}
		catch( SQLException e) {	
			throw new CushyDBException( "An exception occurred while rolling back transaction", e);
		}
	}	
	
	public boolean isClosed(){
		if( jdbcConnection != null){
			try {
				return jdbcConnection.isClosed();
			} 
			catch (SQLException e) {
				return false;
			}
		}
		else{
			return false;
		}			
	}
	
	public void close(){
		
		try {
			jdbcConnection.close();
		} 
		catch (SQLException e) {
			throw new CushyDBException( "An exception occurred while closing database connection", e);
		}
	}
}
