package com.cushydb.bean;

import java.util.ArrayList;
import java.util.List;

import com.cushydb.bean.Parameter.GroupByParameter;
import com.cushydb.bean.Parameter.InsertParameter;
import com.cushydb.bean.Parameter.OrderByParameter;
import com.cushydb.bean.Parameter.SelectFunctionParameter;
import com.cushydb.bean.Parameter.SelectParameter;
import com.cushydb.bean.Parameter.SetParameter;
import com.cushydb.enums.FunctionType;


public class ParameterList {

	private static class ParameterListBase{
		
		//Property
		protected List<ParameterInterface> parameterList;
		
		protected ParameterListBase(){
			parameterList = new ArrayList<ParameterInterface>();
		}
		
		public List<ParameterInterface> getList(){
			return parameterList;
		}
		
		public boolean isEmpty(){
			return parameterList.isEmpty();
		}
	}
		
	
	public static class SelectParameterList extends ParameterListBase{
		
		private SelectParameterList(){}
								
		public SelectParameterList add( SelectParameter p){
			parameterList.add( p);
			return this;
		}
		
		public SelectParameterList add( SelectFunctionParameter p){
			parameterList.add( p);
			return this;
		}
		
		public SelectParameterList add( String parameterName){
			parameterList.add( Parameter.Select( parameterName));
			return this;
		}
		
		public SelectParameterList add( String tableAlias, String parameterName){
			parameterList.add( Parameter.Select( tableAlias, parameterName));
			return this;
		}
		
		public SelectParameterList add( FunctionType functionType, String parameterName){
			parameterList.add( Parameter.SelectFunction( functionType, parameterName));
			return this;
		}
		
		public SelectParameterList add( FunctionType functionType, String tableAlias, String parameterName){
			parameterList.add( Parameter.SelectFunction( functionType, tableAlias, parameterName));
			return this;
		}
	}
	
	public static class GroupByParameterList extends ParameterListBase{
		
		private GroupByParameterList(){}
		
		public GroupByParameterList add( GroupByParameter p){
			parameterList.add( p);
			return this;
		}
		
		public GroupByParameterList addAsc( String parameterName){			
			parameterList.add( Parameter.GroupByAsc( parameterName));			
			return this;
		}
		
		public GroupByParameterList addDesc( String parameterName){		
			parameterList.add( Parameter.GroupByDesc( parameterName));	
			return this;
		}
		
		public GroupByParameterList addAsc( String tableAlias, String parameterName){
			parameterList.add( Parameter.GroupByAsc( tableAlias, parameterName));			
			return this;
		}
		
		public GroupByParameterList addDesc( String tableAlias, String parameterName){						
			parameterList.add( Parameter.GroupByDesc( tableAlias, parameterName));			
			return this;
		}
	}
	
	public static class OrderByParameterList extends ParameterListBase{
		
		private OrderByParameterList(){}
						
		public OrderByParameterList add( OrderByParameter p){
			parameterList.add( p);
			return this;
		}
		
		public OrderByParameterList addAsc( String parameterName){			
			parameterList.add( Parameter.OrderByAsc( parameterName));			
			return this;
		}
		
		public OrderByParameterList addDesc( String parameterName){		
			parameterList.add( Parameter.OrderByDesc( parameterName));	
			return this;
		}
		
		public OrderByParameterList addAsc( String tableAlias, String parameterName){
			parameterList.add( Parameter.OrderByAsc( tableAlias, parameterName));			
			return this;
		}
		
		public OrderByParameterList addDesc( String tableAlias, String parameterName){						
			parameterList.add( Parameter.OrderByDesc( tableAlias, parameterName));			
			return this;
		}
	}
	
	public static class InsertParameterList extends ParameterListBase{
		
		private InsertParameterList(){}
		
		public InsertParameterList add( InsertParameter p){
			parameterList.add( p);
			return this;
		}
		
		public InsertParameterList add( String parameterName, Object parameterValue){
			parameterList.add( Parameter.Insert( parameterName, parameterValue));
			return this;
		}
		
		public InsertParameterList add( String tableAlias, String parameterName, Object parameterValue){
			parameterList.add( Parameter.Insert( tableAlias, parameterName, parameterValue));
			return this;
		}
	}
	
	public static class SetParameterList extends ParameterListBase{
		
		private SetParameterList(){}
		
		public SetParameterList add( SetParameter p){
			parameterList.add( p);
			return this;
		}
		
		public SetParameterList add( String parameterName, Object parameterValue){
			parameterList.add( Parameter.Insert( parameterName, parameterValue));
			return this;
		}
		
		public SetParameterList add( String tableAlias, String parameterName, Object parameterValue){
			parameterList.add( Parameter.Insert( tableAlias, parameterName, parameterValue));
			return this;
		}
	}
	
	//Static factories
	public static SelectParameterList Select(){		
		return new SelectParameterList();
	}
	
	public static GroupByParameterList GroupBy(){		
		return new GroupByParameterList();
	}
	
	public static OrderByParameterList OrderBy(){		
		return new OrderByParameterList();
	}
	
	public static InsertParameterList Insert(){		
		return new InsertParameterList();
	}
	
	public static SetParameterList Set(){		
		return new SetParameterList();
	}	
}
