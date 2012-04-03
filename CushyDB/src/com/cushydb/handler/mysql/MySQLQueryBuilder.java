package com.cushydb.handler.mysql;

import java.util.List;
import java.util.Map.Entry;

import com.cushydb.bean.Container;
import com.cushydb.bean.Join;
import com.cushydb.bean.Parameter.ConstraintFunctionParameter;
import com.cushydb.bean.Parameter.ConstraintParameter;
import com.cushydb.bean.Parameter.GroupByParameter;
import com.cushydb.bean.Parameter.OrderByParameter;
import com.cushydb.bean.Parameter.SelectFunctionParameter;
import com.cushydb.bean.Parameter.SelectParameter;
import com.cushydb.bean.Parameter.SetParameter;
import com.cushydb.bean.ParameterInterface;
import com.cushydb.bean.ParameterList.GroupByParameterList;
import com.cushydb.bean.ParameterList.OrderByParameterList;
import com.cushydb.bean.ParameterList.SelectParameterList;
import com.cushydb.bean.ParameterList.SetParameterList;
import com.cushydb.bean.TableInfo;
import com.cushydb.common.CushyDBUtils;
import com.cushydb.enums.CompareType;

public class MySQLQueryBuilder {

	private MySQLQueryBuilder(){}
	
	public static void generateSelectClause( SelectParameterList parameterList, StringBuilder stringBuilder){
				
		for( ParameterInterface pi: parameterList.getList()){
			
			if( pi instanceof SelectParameter){
				stringBuilder.append( generateSelectClause( (SelectParameter)pi));
			}
			else if( pi instanceof SelectFunctionParameter){
				stringBuilder.append( generateSelectFunctionClause((SelectFunctionParameter)pi));
			} 							
			stringBuilder.append( ", ");			
		}
		
		if( !parameterList.getList().isEmpty()){
			stringBuilder = stringBuilder.delete( stringBuilder.length()-2, stringBuilder.length());
		}
	}
	
	private static String generateSelectClause( SelectParameter p){
		
		String clause =  p.getTableAlias() + "." + p.getParameterName();
		if( p.getParameterNameAlias() != null){
			clause += " " + p.getParameterNameAlias();
		}
		return clause;
	}
	
	private static String generateSelectFunctionClause( SelectFunctionParameter p){
		
		String clause =  p.getFunctionType() + "(" + p.getTableAlias() + "." + p.getParameterName() + ")";
		if( p.getParameterNameAlias() != null){
			clause += " " + p.getParameterNameAlias();
		}
		return clause;
	}
	
	public static void generateFromOrUpdateClause( TableInfo tableInfo, StringBuilder stringBuilder){
		
		if( tableInfo.isSingleTable()){
			
			stringBuilder.append( CushyDBUtils.generateFromUnit( tableInfo.getSingleTableName(), tableInfo.getSingleTableAlias()));
		}
		else{
			for( Entry<String,String> entry : tableInfo.getTableAliasToTableNameMap().entrySet()){
				
				stringBuilder.append( CushyDBUtils.generateFromUnit( entry.getValue(), entry.getKey()));
				stringBuilder.append( ", ");			
			}
			
			//Remove trailing comma
			if( !tableInfo.getTableAliasToTableNameMap().isEmpty()){
				stringBuilder = stringBuilder.delete( stringBuilder.length()-2, stringBuilder.length());			
			}
		}
	}
	
	public static void generateWhereOrHavingClause( Container container, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		List<Container> childContainerList = container.getContainerList();
		List<Join> joinList = container.getJoinList();
		List<ParameterInterface> parameterList = container.getParameterList();
		
		String strContainerTypeStr = container.getContainerType().toString();
		int unnecessaryTrailerSize = strContainerTypeStr.length() + 2;
		
		if( !parameterList.isEmpty() || !joinList.isEmpty() || !childContainerList.isEmpty()){
			
			stringBuilder.append('(');
			
			for( ParameterInterface pi: parameterList){
							
				stringBuilder.append( generateConstraintUnitAndUpdateParameterList( pi, parameterValueListInSequence));
				stringBuilder.append( ' ');
				stringBuilder.append( strContainerTypeStr);	
				stringBuilder.append( ' ');				
			}
			
			for( Join j: joinList){
			
				stringBuilder.append( j.generateConstraintUnit());
				stringBuilder.append( ' ');
				stringBuilder.append( strContainerTypeStr);	
				stringBuilder.append( ' ');
			}			
									
			for( Container childContainer: childContainerList){
				
				generateWhereOrHavingClause( childContainer, stringBuilder, parameterValueListInSequence);
				stringBuilder.append( ' ');
				stringBuilder.append( strContainerTypeStr);	
				stringBuilder.append( ' ');
			}
			
			//Remove last strContainerTypeStr + " "							
			stringBuilder = stringBuilder.delete( stringBuilder.length() - unnecessaryTrailerSize, stringBuilder.length());			
						
			stringBuilder.append(')');
		}		
	}
	
	public static void generateSetClause( SetParameterList setParameterList, StringBuilder stringBuilder, List<Object> parameterValueListInSequence){
		
		for( ParameterInterface pi: setParameterList.getList()){
						
			stringBuilder.append( generateConstraintUnitAndUpdateParameterList( pi, parameterValueListInSequence));
			stringBuilder.append( ", ");
		}
		
		stringBuilder = stringBuilder.delete( stringBuilder.length() - 2, stringBuilder.length());	
	}
	
	//Constructs a constraint clause with given parameter. 
	//If parameter is null or it is a list and it contains null values they are not added to parameterValueListInSequence
	//They are embedded into sql as NULL.
	private static String generateConstraintUnitAndUpdateParameterList( ParameterInterface pi, List<Object> parameterValueListInSequence){
		
		String clause = "";
		
		if( pi instanceof ConstraintParameter){
			clause = generateConstraintParameterClause( (ConstraintParameter)pi, parameterValueListInSequence);
		}
		if( pi instanceof ConstraintFunctionParameter){
			clause = generateConstraintFunctionParameterClause( (ConstraintFunctionParameter)pi, parameterValueListInSequence);
		}
		else if( pi instanceof SetParameter){
			clause = generateSetParameterClause( (SetParameter)pi, parameterValueListInSequence);
		}
		
		return clause;	
	}
		
	private static String generateConstraintParameterClause( ConstraintParameter p, List<Object> parameterValueListInSequence){
		
		StringBuilder stringBuilder = new StringBuilder();
		
		if( p.getParameterValue() == null){
			stringBuilder.append( p.getTableAlias() + "." + p.getParameterName() + " " + p.getCompareType().getJDBCValue() + " NULL");
		}
		//If compareType is IN/NOT IN insert the parameterValue object[] comma separated
		else if( p.getParameterValue() != null && 
				 p.getParameterValue() instanceof Object[] && 
				(p.getCompareType() == CompareType.IN || p.getCompareType() == CompareType.NOT_IN)){
			
			stringBuilder.append( p.getTableAlias() + "." + p.getParameterName() + " " + p.getCompareType().getJDBCValue() + " (");
						
			for( Object parameterValue: (Object[])p.getParameterValue()){
				
				if( parameterValue == null){
					stringBuilder.append( "NULL,");
				}
				else{
					stringBuilder.append( "?,");
					parameterValueListInSequence.add( parameterValue);
				}
			}
			stringBuilder = stringBuilder.delete( stringBuilder.length()-1, stringBuilder.length());
			stringBuilder.append(")");
		}
		else if( p.getParameterValue() != null && !(p.getParameterValue() instanceof Object[])){
					
			stringBuilder.append( p.getTableAlias() + "." + p.getParameterName() + " " + p.getCompareType().getJDBCValue() + " ?");					
			parameterValueListInSequence.add( p.getParameterValue());
		}
		
		return stringBuilder.toString();
	}
	
	private static String generateConstraintFunctionParameterClause( ConstraintFunctionParameter p, List<Object> parameterValueListInSequence){
		
		StringBuilder stringBuilder = new StringBuilder();
				
		if( p.getParameterValue() != null){
			stringBuilder.append( p.getFunctionType() + "(" + p.getTableAlias() + "." + p.getParameterName() + ") " + p.getCompareType().getJDBCValue() + " ?");				
			parameterValueListInSequence.add( p.getParameterValue());
		}
		
		return stringBuilder.toString();
	}
	
	private static String generateSetParameterClause( SetParameter p, List<Object> parameterValueListInSequence){
		
		parameterValueListInSequence.add( p.getParameterValue());
		return p.getTableAlias() + "." + p.getParameterName() + " = ?"; 
	}	
	
	public static void generateGroupByClause( GroupByParameterList parameterList, StringBuilder stringBuilder){
						
		for( ParameterInterface pi: parameterList.getList()){
						
			GroupByParameter p = (GroupByParameter)pi;		
			
			stringBuilder.append( generateGroupOrOrderByClause( p.getTableAlias(), p.getParameterName(), p.isAscending()));					
			stringBuilder.append(", ");
		}
		
		//Remove last ", "
		stringBuilder = stringBuilder.delete( stringBuilder.length() - 2, stringBuilder.length());			
	}
	
	public static void generateOrderByClause( OrderByParameterList parameterList, StringBuilder stringBuilder){
		
		for( ParameterInterface pi: parameterList.getList()){
						
			OrderByParameter p = (OrderByParameter)pi;		
			
			stringBuilder.append( generateGroupOrOrderByClause( p.getTableAlias(), p.getParameterName(), p.isAscending()));					
			stringBuilder.append(", ");
		}
		
		//Remove las ", "
		stringBuilder = stringBuilder.delete( stringBuilder.length() - 2, stringBuilder.length());			
	}
	
	private static String generateGroupOrOrderByClause( String tableAlias, String parameterName, boolean isAscending){
		
		StringBuilder stringBuilder = new StringBuilder();		
		stringBuilder.append( tableAlias + "." + parameterName);
		
		if( isAscending){
			stringBuilder.append( " ASC");
		}
		else{
			stringBuilder.append( " DESC");
		}	
		
		return stringBuilder.toString();
	}
}
