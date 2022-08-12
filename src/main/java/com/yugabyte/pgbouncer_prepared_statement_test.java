package com.yugabyte;

import java.lang.Math;   
import java.util.Random;

//  Regarding sql
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;


public class pgbouncer_prepared_statement_test{
  public static void reset_db(){
    try {
        try{
          Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
          System.err.println(e.getMessage());
        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5400/db1?binary_parameters=yes",
                                                      "user1", "user1pass");
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS test_table");
        stmt.execute("CREATE TABLE IF NOT EXISTS test_table" +
                    "  (id int primary key, test_name varchar, thread_id int )");
        conn.close();

    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException  {

    //  Reset the database
    reset_db();
    
    
    System.out.println("Database has been initialized");
    int n = 10; // Number of connections

    //Make all the connections 
    Connection conn[] = new Connection[n] ;
    
    for (int i = 0; i < n; i++) {

      try {
        conn[i] = DriverManager.getConnection("jdbc:postgresql://localhost:5400/db1?binary_parameters=yes","user1", "user1pass" );
        //conn[i].setAutoCommit(false);
      
      } catch (SQLException e) {
        System.err.println("Error in creating Connection" + e.getMessage());
      }
    }
    
    // Create all the prepared statements 
    PreparedStatement insert_stmt[]  = new  PreparedStatement[n];
    PreparedStatement select_stmt[]  = new  PreparedStatement[n];

    for(int i=0;i<n;i++)
    {
      try{
      insert_stmt[i] =conn[i].prepareStatement("INSERT INTO test_table VALUES (?, 'Prepared_Statement', ? )");  
      select_stmt[i] =conn[i].prepareStatement("select count(*) from test_table where thread_id=?");       
      }
      catch (Exception e)
      {
        System.err.println("Error in creating Prepared Statements" + e.getMessage());
      }
    }
  
    System.out.println("Created the Prepared Statements");
    
    // Create the objects 
    TestPreparedStatement test_objs[] = new TestPreparedStatement[n];
    for(int i=0;i<n;i++)
    {
      test_objs[i] =  new TestPreparedStatement(i,conn[i],insert_stmt[i],select_stmt[i]);
    }    

    
    System.out.println("Created Test Objects");

    Thread threads[] = new Thread[n];
    for(int i =0;i<n;i++)
    threads[i] =  new Thread(test_objs[i]);

    
    //  Run all the threads 
    try { 
        for(int i =0;i<n;i++)
      {
        threads[i].start();
      }

      //  Join all the threads 

      for(int i =0;i<n;i++)
      {
        threads[i].join();
      }

    }catch(Exception e)
    {
      System.out.println("Error in creating Threads" + e.getMessage());
    }
    
    System.out.println("Test done");

    //  Close all the connections 
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

class TestPreparedStatement implements Runnable {
  public volatile int index;
  public volatile Connection conn;
  public volatile PreparedStatement insert_stmt, select_stmt;
  
  public TestPreparedStatement( int index,  Connection conn, PreparedStatement insert_stmt, PreparedStatement select_stmt )
  {
    this.index = index; 
    this.conn = conn;
    this.insert_stmt = insert_stmt ; 
    this.select_stmt = select_stmt ; 

  }

  public void run(){
    int times = 0 ; 

    while(times < 100 )
    {
      times++ ; 

      int statementExe = times%2 ; 
 
      try{
        //"INSERT INTO test_table VALUES (?, 'Prepared_Statement', ? )"  
        //"select count(*) from test_table where thread_id=?"

        if(statementExe == 0) //statementExe == 0
        {
          //  Execute Insert  
          this.insert_stmt.setInt(1,index*10000 + times); 
          this.insert_stmt.setInt(2,index);
          int ret_val = this.insert_stmt.executeUpdate();
        //  System.out.println(ret_val);

        }else 
        {
          //  Execute Select Statement
          this.select_stmt.setInt(1,index);
          ResultSet rs = this.select_stmt.executeQuery();
          if(rs.next())
            {
          //    System.out.println("Count for index = "+ index + " is " + rs.getInt(1) );
            } 
            else 
            {
              System.out.println("Count = 0 for index = "+ index) ; 
            }
        }

        //Close all the connections 
        //conn.commit();

    }catch(Exception e)
    {
      e.printStackTrace();
      System.err.println("Getting error while running the thread" + e.getMessage());
      
  //    System.exit(1);
    }
  }
  
  
  }
}

