package com.cushydb.handler;

import com.cushydb.bean.Container;
import com.cushydb.bean.TableInfo;

public interface DBDeleteHandler {

	public DBDeleteHandler Delete();
	public DBDeleteHandler Delete( String deleteTableAlias);
	public DBDeleteHandler From( TableInfo tableInfo);
	public DBDeleteHandler Where( Container constraintContainer);
	public boolean execute();
	public String sql();
}
