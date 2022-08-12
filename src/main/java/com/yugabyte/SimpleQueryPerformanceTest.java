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

public class SimpleQueryPerformanceTest{
  public static void reset_db(){
    try {
        try{
          Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
          System.err.println(e.getMessage());

        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5400/yugabyte",
                                                      "yugabyte", "yugabyte");
        Statement stmt = conn.createStatement();

        stmt.execute("DROP TABLE IF EXISTS test_table");
        
        stmt.execute("CREATE TABLE IF NOT EXISTS test_table" +
                    "  (id decimal primary key, test_name varchar, thread_id int )");

    
                  } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException  {
  
    int i = Integer.parseInt(args[0]);
        System.out.printf("Time take by %d number of connections :: %d\n",i,test(i));
  }
  
  public static long test(int n)
  {
        //  Reset the database
        reset_db();
    
        Tester5 test_objs[] = new Tester5[n];
        for(int i=0;i<n;i++)
        {
          test_objs[i] =  new Tester5(i);
        }    
    
        
        System.out.println("Created Test Objects");
    
        Thread threads[] = new Thread[n];
        for(int i =0;i<n;i++)
        threads[i] =  new Thread(test_objs[i]);
    
        try { 
            long start = System.currentTimeMillis();
            for(int i =0;i<n;i++)
                threads[i].start();
    
            for(int i =0;i<n;i++)
                threads[i].join();
            long end = System.currentTimeMillis() ;
            long time_taken =  (end - start) ;
       
            return time_taken;
            
    
        }catch(Exception e)
        {
          e.printStackTrace();
        }
        return 0;
  }

}

class TestObject5 {
  public volatile int index;
  public volatile String  connection_string ; 
  public volatile Connection conn = null ;
  public TestObject5(int index)
  {
    this.index = index;
  }
  TestObject5(int index, String connection_string)
  {
    this.index = index;
    this.connection_string = connection_string;
    
    try{
      this.conn = DriverManager.getConnection(this.connection_string,"yugabyte", "yugabyte"); 
      //conn.setAutoCommit(false);
      //System.out.printf("Created the connectins for the index %d \n",this.index);
    }catch(Exception e)
    {
      e.printStackTrace();
    }
  }
  

  /*  Test Parameters */
  public volatile String SimpleDeleteStatementParser = "Delete from test_table  where id = %d" ;                  //DELETE
  public volatile String SimpleInsertStatementParser = "INSERT INTO test_table VALUES (%d, 'Prepared_Statement', %d )" ;  //INSERT
  public volatile String SimpleUpdateStatementParser = "UPDATE test_table SET test_name = %d where thread_id = %d " ;      //UPDATE
  public volatile String SimpleSelectStatementParser = "Select * from test_table  where id = %d" ;                  //SELECT
  
  public volatile int loopSize = 200;

  /*  Test Function */
  public boolean TestExtendedQuery()
  {
    
    Statement stmt = null;
    
    /*  Prepare the prepareStatements */

    try{

      stmt =  conn.createStatement();
      //conn.commit();
    }catch(Exception e)
    {
      e.printStackTrace();
      return false ;
    }
    Random rand = new Random();
    int times = loopSize-1;
  //  int lastInsert = 0;
    while(times> 0)
    {
      times--;
      try {
       // Thread.sleep(10) ; //10 ms 
      }catch(Exception e)
      {
        e.printStackTrace();
      }
      int rand_int = rand.nextInt(2);
      int val =times+loopSize*this.index ;
      try{
      switch (rand_int) {
        case 1:
            stmt.executeUpdate(String.format(SimpleInsertStatementParser, val, rand_int));
            break;
        case 0:
            ResultSet rs = stmt.executeQuery(String.format(SimpleSelectStatementParser,val - rand_int*20101));
        default:
            break;
    }
    }catch(Exception e)
    {
        e.printStackTrace();
        System.exit(1);
    }

    }
    try{
    //stmt.close();
    //conn.close();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
    return true ; 
  }
}

class Tester5 implements Runnable {
  public volatile int index;
  public volatile TestObject5 test_obj ;
  public Tester5( int index )
  {
    //Set the index
    this.index = index; 
    this.test_obj =  new TestObject5(this.index, "jdbc:postgresql://localhost:5400/yugabyte");
    
  }

  public void run(){
    /*  Create the object */
    this.test_obj.TestExtendedQuery();

  }

}
