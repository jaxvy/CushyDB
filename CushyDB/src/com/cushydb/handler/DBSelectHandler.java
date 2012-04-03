package com.cushydb.handler;

import com.cushydb.bean.Container;
import com.cushydb.bean.ParameterList.GroupByParameterList;
import com.cushydb.bean.ParameterList.OrderByParameterList;
import com.cushydb.bean.ParameterList.SelectParameterList;
import com.cushydb.bean.Result;
import com.cushydb.bean.TableInfo;

public interface DBSelectHandler {

	public DBSelectHandler Select( SelectParameterList selectParameterList);
	public DBSelectHandler Distinct();
	public DBSelectHandler From( TableInfo tableInfo);
	public DBSelectHandler Where(  Container constraintContainer);	
	public DBSelectHandler GroupBy( GroupByParameterList groupByParameterList);	
	public DBSelectHandler Having( Container havingContainer);
	public DBSelectHandler OrderBy( OrderByParameterList orderByParameterList);	
	public DBSelectHandler Limit( int limit);	
	public DBSelectHandler Limit( int limit, int offset);
	
	public Result execute();	
	public String sql();
}
