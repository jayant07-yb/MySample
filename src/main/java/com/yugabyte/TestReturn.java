package com.yugabyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class TestReturn{
  public static void reset_db()
  {
    try {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5400/db1",
                                                      "user1", "user1pass");
        Statement stmt = conn.createStatement();
    
      
        stmt.execute("DROP TABLE IF EXISTS employee");
        stmt.execute("CREATE TABLE IF NOT EXISTS employee" +
                    "  (id int primary key, name varchar, age int, language text)");
        
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }
  public static void main(String[] args) throws ClassNotFoundException, SQLException  {

    try{
      Class.forName("org.postgresql.Driver");
    }
    catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
    }


    reset_db();
    System.out.println("Database has been initialized");
    int n = 1; // Number of connections

    //Make all the connections 
    Connection conn[] = new Connection[n] ;

    //  Check for max number of connections and 
    for (int i = 0; i < n; i++) {
      String hostid = "localhost:5400";
      String sf1=String.format("jdbc:postgresql://%s/db1", hostid);
      try {
        conn[i] = DriverManager.getConnection(sf1,"user1", "user1pass" );
      } catch (SQLException e) {
        System.err.println(e.getMessage());
      }
    }

    System.out.println("All connections are made");

    try{
      /*Statement stmtx = conn[0].createStatement() ;
      ResultSet rsx = stmtx.executeQuery("PREPARE Aasd2 AS INSERT INTO employee VALUES ($1, 'John', $2, 'Prepared_Statement')");
      
      System.exit(0);
      */
      long start_1 = System.currentTimeMillis();  
      PreparedStatement stmt[]  = new  PreparedStatement[2];
      
      //  Do Insert Query and measure average time.  
      for (int i = 0; i < n; i++) {
        try {
          /*
          Statement stmt = conn[i].createStatement();
          String insertStr = String.format("INSERT INTO employee VALUES (%d, 'John', 35, 'Java')",i+1);
          stmt.execute(insertStr);
          */

          stmt[0] =conn[i].prepareStatement("INSERT INTO employee VALUES (?, 'John', 35, 'Prepared_Statement')");  
          
          stmt[1] =conn[i].prepareStatement("INSERT INTO employee VALUES (?, 'John', ?, 'Prepared_Statement')");  
          
        } catch (SQLException e) {
          System.err.println(e.getMessage());
        }
      }


      for(int i =0 ; i < 10 ;i++)
      {
        try{
          stmt[0].setInt(1,i+ n*10);//1 specifies the first parameter in the query  
          stmt[1].setInt(1,i+ n*100);//1 specifies the first parameter in the query  
          stmt[1].setInt(2,i+ n*100);//1 specifies the first parameter in the query  

          int isf=stmt[0].executeUpdate();  
          int isf2=stmt[1].executeUpdate();  
          System.out.println(isf);   
          System.out.println(isf2); 

        }catch( SQLException e)
        {
          System.err.println(e.getMessage()); 
        }
      }
      //stmt[0].close();
      //stmt[0] =conn[0].prepareStatement("INSERT INTO employee VALUES (?, 'John', 35, 'Prepared_Statement')");  
        
      for(int i =0 ; i < 10 ;i++)
      {
        try{
          stmt[0].setInt(1,i+ n*1000);//1 specifies the first parameter in the query  
          stmt[1].setInt(1,i+ n*10000);//1 specifies the first parameter in the query  
          stmt[1].setInt(2,i+ n*10000);//1 specifies the first parameter in the query  

          int isf=stmt[0].executeUpdate();  
          int isf2=stmt[1].executeUpdate();  
          System.out.println(isf);   
          System.out.println(isf2); 

        }catch( SQLException e)
        {
          System.err.println(e.getMessage()); 
        }
      }

      long end_1 = System.currentTimeMillis();  

      System.out.println("Elapsed Time in milli seconds: "+ (end_1-start_1));
    }
    catch(Exception e)
    {
      System.err.println(e.getMessage());
    }
    

    //Transaction 
/*
    try{
      for(int i =0 ;i<n;i++)
      conn[i].setAutoCommit(false);  

      Statement stmt2[] =  new Statement[n]  ;

      for(int i =0 ;i<n;i++)
      stmt2[i]=conn[i].createStatement();  
      for(int i =0 ;i<n;i++)
      stmt2[i].executeUpdate( String.format("INSERT INTO employee VALUES (%d, 'John', 35, 'Transaction')",i+10000));  
      for(int i =0 ;i<n;i++)
      stmt2[i].executeUpdate( String.format("INSERT INTO employee VALUES (%d, 'John', 35, 'Transaction')",i*100 + 100009));  
      for(int i =0 ;i<n;i++)
      conn[i].commit();  


    }catch (Exception e)
    {
      System.err.println(e.getMessage()); 
    }
*/
    //Close all the Connections 

    try{
      for (int i = 0; i < n; i++) {
        try {
          conn[i].close();
        } catch (SQLException e) {
          System.err.println(e.getMessage());
        }
      }
    }
    catch(Exception e)
    {
      System.err.println(e.getMessage());
    }
    
  }
}
