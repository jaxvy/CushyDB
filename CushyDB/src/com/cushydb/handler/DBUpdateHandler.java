package com.cushydb.handler;

import com.cushydb.bean.Container;
import com.cushydb.bean.ParameterList.SetParameterList;
import com.cushydb.bean.TableInfo;

public interface DBUpdateHandler {

	public DBUpdateHandler Update( TableInfo tableInfo);	
	public DBUpdateHandler Set( SetParameterList setParameterList);
	public DBUpdateHandler Where( Container constraintContainer);
	public boolean execute();
	public String sql();
	
}
