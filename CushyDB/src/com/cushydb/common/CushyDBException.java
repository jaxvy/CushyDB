package com.cushydb.common;

public class CushyDBException extends RuntimeException {

	public CushyDBException(){
		super();
	}
	
	public CushyDBException( String message){
		super( message);		
	}
	
	public CushyDBException( Throwable e){
		super( e);
	}
	
	public CushyDBException( String message, Throwable e){
		super( message, e);		
	}		
}
