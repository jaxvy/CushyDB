package com.cushydb.handler;


import com.cushydb.bean.ParameterList.InsertParameterList;
import com.cushydb.bean.Result;

public interface DBInsertHandler {

	public DBInsertHandler Insert( InsertParameterList insertParameterList);	
	public DBInsertHandler Into( String tableName);
	public DBInsertHandler ReturnKey();
	public DBInsertHandler AddBatch( Object[] batchInsertParameters);	
	public DBInsertHandler BatchSize( int batchSize);	
	
	public Result execute();
	public String queryString(); 
}
