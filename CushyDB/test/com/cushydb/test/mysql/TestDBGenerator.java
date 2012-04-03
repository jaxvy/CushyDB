package com.cushydb.test.mysql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDBGenerator {

	public static void createTables( Connection databaseConnection) throws SQLException{
		
		Statement statement = databaseConnection.createStatement();				
		statement.executeUpdate("DROP TABLE IF EXISTS GRADE;");
		statement.executeUpdate("DROP TABLE IF EXISTS COURSE;");
		statement.executeUpdate("DROP TABLE IF EXISTS STUDENT;");
		
		String sql1 = "CREATE TABLE STUDENT (ID INT NOT NULL AUTO_INCREMENT, FIRSTNAME VARCHAR(64), LASTNAME VARCHAR(64), BIRTHDATE DATETIME, GPA DOUBLE, PRIMARY KEY (ID))";
		String sql2 = "CREATE TABLE COURSE (ID INT NOT NULL AUTO_INCREMENT, NAME VARCHAR(64), PRIMARY KEY (ID))";
		String sql3 = "CREATE TABLE GRADE (STUDENT_ID INT, COURSE_ID INT, GRADE VARCHAR(2), CONSTRAINT FK_STUDENT FOREIGN KEY ( STUDENT_ID ) REFERENCES STUDENT (ID ), CONSTRAINT FK_COURSE FOREIGN KEY ( COURSE_ID ) REFERENCES COURSE (ID ));";
		statement.executeUpdate(sql1);
		statement.executeUpdate(sql2);
		statement.executeUpdate(sql3);		
	}
	
	@SuppressWarnings("deprecation")
	public static void insertTestData( Connection databaseConnection) throws SQLException{
				
		List<Student> studentList = new ArrayList<Student>();
		studentList.add( new Student("firstname_1", "lastname_1", new Date(80, 1, 1), 2.1));
		studentList.add( new Student("firstname_2", "lastname_2", new Date(80, 1, 2), 2.2));
		studentList.add( new Student("firstname_3", "lastname_3", new Date(80, 1, 3), 2.3));
		studentList.add( new Student("firstname_4", "lastname_4", new Date(80, 1, 4), 2.4));
		studentList.add( new Student("firstname_5", "lastname_5", new Date(80, 1, 5), 2.5));
		studentList.add( new Student("firstname_6", "lastname_6", new Date(80, 1, 6), 2.6));
		studentList.add( new Student("firstname_7", "lastname_7", new Date(80, 1, 7), 2.7));
		studentList.add( new Student("firstname_8", "lastname_8", new Date(80, 1, 8), 2.8));
		
		PreparedStatement preparedStatement = databaseConnection.prepareStatement("INSERT INTO STUDENT (FIRSTNAME, LASTNAME, BIRTHDATE, GPA) VALUES (?,?,?,?)", PreparedStatement.RETURN_GENERATED_KEYS);
		for( Student student: studentList){
			
			preparedStatement.setString(1, student.firstname);
			preparedStatement.setString(2, student.lastname);
			preparedStatement.setDate(3, student.birthdate);
			preparedStatement.setDouble(4, student.gpa);
			
			preparedStatement.executeUpdate();
						
			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			if( resultSet.next()){
				student.id = resultSet.getInt(1);
			}
		}
		
		
		List<Course> courseList = new ArrayList<Course>();
		courseList.add( new Course( "course_1"));
		courseList.add( new Course( "course_2"));
		courseList.add( new Course( "course_3"));
		courseList.add( new Course( "course_4"));
		
		
		preparedStatement = databaseConnection.prepareStatement("INSERT INTO COURSE (NAME) VALUES (?)", PreparedStatement.RETURN_GENERATED_KEYS);
		for( Course course: courseList){
			
			preparedStatement.setString(1, course.name);
						
			preparedStatement.executeUpdate();
						
			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			if( resultSet.next()){
				course.id = resultSet.getInt(1);
			}
		}
		
		Map<Integer, String> studentGradeMap = new HashMap<Integer, String>();
		studentGradeMap.put(1, "A");
		studentGradeMap.put(2, "A-");
		studentGradeMap.put(3, "B+");
		studentGradeMap.put(4, "B");
		
		preparedStatement = databaseConnection.prepareStatement("INSERT INTO GRADE (STUDENT_ID, COURSE_ID, GRADE) VALUES (?,?,?)");
		for( int i = 0; i < courseList.size(); i++){
			
			for( int j = 0; j < (i+1)*2; j++){
				
				int studentID = studentList.get(j).id;
				int courseID = courseList.get(i).id;
				String grade = studentGradeMap.get(i + 1);
				
				preparedStatement.setInt( 1, studentID);
				preparedStatement.setInt( 2, courseID);
				preparedStatement.setString(3, grade);	
				
				preparedStatement.executeUpdate();
			}			
		}		
	}
	
	public static class Student{
		
		public int id;
		public String firstname;
		public String lastname;
		public Date birthdate;
		public double gpa;
		
		public Student( String firstname, String lastname, Date birthdate, double gpa) {
			this.firstname = firstname;
			this.lastname = lastname;
			this.birthdate = birthdate;
			this.gpa = gpa;
		}	
	}
	
	public static class Course{
		
		public int id;
		public String name;
		
		public Course(String name) {
			this.name = name;
		}	
	}		
}
