package com.cushydb.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.cushydb.common.CushyDBConnection;
import com.cushydb.common.CushyDBException;


public class DBConnectionHandler {

	//Properties
	private static final DBConnectionHandler instance = new DBConnectionHandler();
	private CushyDBConnection cushyDbConnection;
	
	private DBConnectionHandler(){
		
		if( cushyDbConnection == null){
						
			initializeDatabaseConnection();
		}
	}
	
	public static CushyDBConnection getConnection(){	
		
		return instance.cushyDbConnection;
	}


	private void initializeDatabaseConnection(){
		
		try{			
			//Read in values from config file
			File file = new File("config/db_config.xml");
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document document = db.parse(file);
			
			String driverClass = null;
			Node node = null;
			NodeList nodeList = document.getElementsByTagName("driver_class");
			if( nodeList != null && nodeList.getLength() > 0){
				node = nodeList.item(0);
				driverClass = node.getTextContent();
			}
			
			nodeList = document.getElementsByTagName("database_provider");
			node = nodeList.item(0);
			String databaseProvider = node.getTextContent();
			
			nodeList = document.getElementsByTagName("host_address");
			node = nodeList.item(0);
			String hostAddress = node.getTextContent();
			
			nodeList = document.getElementsByTagName("host_port");
			node = nodeList.item(0);
			String hostPort = node.getTextContent();
			
			nodeList = document.getElementsByTagName("database_name");
			node = nodeList.item(0);
			String databaseName = node.getTextContent();
			
			nodeList = document.getElementsByTagName("username");
			node = nodeList.item(0);
			String username = node.getTextContent();
			
			nodeList = document.getElementsByTagName("password");
			node = nodeList.item(0);
			String password = node.getTextContent();
			
			cushyDbConnection = new CushyDBConnection();
			
			if( databaseProvider.equals("mysql")){
			
				initJDBCConnection( driverClass, databaseProvider, hostAddress, hostPort, databaseName, username, password);
			}	
		}
		catch( Exception e){
			throw new CushyDBException("An exception has occurred while establishing connection with the database, check db_config.xml", e);
		}	
	}
	
	private void initJDBCConnection( String driverClass, String databaseProvider, 
									 String hostAddress, String hostPort, String databaseName, 
									 String username, String password) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
				
		//Initialize the database driver
		Class.forName( driverClass).newInstance();
		
		// Get a connection to the database
		String connectionAddress = "jdbc:" + databaseProvider + "://" + hostAddress + ":" + hostPort + "/" + databaseName + "?user=" + username + "&password=" + password;
		
		Connection databaseConnection = DriverManager.getConnection(connectionAddress);						
		databaseConnection.setAutoCommit( false);
		
		cushyDbConnection.setJDBCConnection( databaseConnection);
	}
	
	public static void close( PreparedStatement preparedStatement){
		
		try{
			if( preparedStatement != null){
				preparedStatement.close();
			}
		}
		catch( SQLException e){
			throw new CushyDBException("Unable to close preparedStatement", e);
		}		
	}

	public static void close( ResultSet resultSet, PreparedStatement preparedStatement){
		
		try{
			if( resultSet != null){
				resultSet.close();
			}
			
			if( preparedStatement != null){
				preparedStatement.close();
			}
		}
		catch( SQLException e){
			throw new CushyDBException("Unable to close resultSet and preparedStatement", e);
		}		
	}	
}
