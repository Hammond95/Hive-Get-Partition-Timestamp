package hivetests;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.text.*;
import java.util.*;
import java.lang.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main2 {

	public static void main(String[] args) throws SQLException {
		
		final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";  
		
		try 
		{	Class.forName(JDBC_DRIVER);
		}
		catch (ClassNotFoundException e2)
		{	e2.printStackTrace();
		}
		
		final String DB_URL = "JDBC CONNECTION STRING";
		final String USER = "USERNAME";
		final String PASS = "PASSWORD";
		
		Connection connectionObj = DriverManager.getConnection(DB_URL,USER,PASS);
		
		
		String dbName="DB NAME";
		String tableName="TABLE_NAME";
		String partitionValue="PARTITION VALUE";
		
		String dataStr = getCreateTime(connectionObj,dbName,tableName,partitionValue);
		System.out.println(dataStr);
		
		connectionObj.close();
	}
	
	public static String getCreateTime(Connection connectionObj, String dbName, String tableName, String partitionValue) throws SQLException
	{
		//Regex Pattern
		String regex = "(CreateTime:)(.+)(([a-zA-Z]{3}[ ]){2}[\\d]{2}[ ](\\d{2}:){2}\\d{2}[ ][a-zA-Z]{3}[ ]\\d{4})";
		
		Pattern p = Pattern.compile(regex);
		Matcher m;

		//Default Format returned from the query
		DateFormat TCTFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",Locale.ENGLISH);
		//Default Output Format
		SimpleDateFormat outFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		Date outFormatDate;
		
		//Default Output Date Value
		String dataStr="01-01-1900 00:00:01";
		
		if(connectionObj!=null)
		{	Statement stmtObj = null;
			try 
			{	stmtObj = connectionObj.createStatement();
			} 
			catch (SQLException e1) 
			{	e1.printStackTrace();
			}
			
			if((!dbName.trim().isEmpty()) && (!tableName.trim().isEmpty()) && (!partitionValue.trim().isEmpty()))
			{	String sql = "DESCRIBE FORMATTED "+dbName+"."+tableName+" partition(snapshot_date="+partitionValue+")";
			    ResultSet rs = null;
				try {
					rs = stmtObj.executeQuery(sql);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				
			    while(rs.next())
			    {	String columnValue = rs.getString(1) + rs.getString(2) + rs.getString(3);
			        m = p.matcher(columnValue);
			        if(m.find())
			        {	String TCTime = m.group(3);
			            try 
			            {   outFormatDate = TCTFormat.parse(TCTime);
			                dataStr = outFormat.format(outFormatDate);
			            }
			            catch (ParseException e)
			            {    e.printStackTrace();
			            }
			         }
			     }
			}
			stmtObj.close();
		}
		
		return dataStr;
	}

	public static String getMaxPartition(Connection connectionObj, String dbName, String tableName) throws SQLException
	{	
		//Default Output Date Value
		String maxSnapshotDate="19000101";
		
		if(connectionObj!=null)
		{	Statement stmtObj = null;
			try 
			{	stmtObj = connectionObj.createStatement();
			} 
			catch (SQLException e1) 
			{	e1.printStackTrace();
			}

			if((!dbName.trim().isEmpty()) && (!tableName.trim().isEmpty()))
			{   String sql = "SELECT MAX(SNAPSHOT_DATE) FROM "+dbName+"."+tableName+";";
		     	ResultSet rs = stmtObj.executeQuery(sql);
		     	if(rs.next())
		     	{   maxSnapshotDate = rs.getString(1);
		     	}
			}
			stmtObj.close();
		}
		return maxSnapshotDate;
		
	}	
}
