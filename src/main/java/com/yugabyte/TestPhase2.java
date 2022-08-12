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

public class TestPhase2{
  public static void reset_db(){
    try {
        try{
          Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
          System.err.println(e.getMessage());

        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5400/db1",
                                                      "user1", "user1pass");
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS test_table");
        stmt.execute("CREATE TABLE IF NOT EXISTS test_table" +
                    "  (id decimal primary key, test_name varchar, thread_id int )");
        conn.close();

    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException  {

    //  Reset the database
    reset_db();
    
    int n = 300; // Number of connections
    // Create all the prepared statements     
    // Create the objects 
    Tester2 test_objs[] = new Tester2[n];
    for(int i=0;i<n;i++)
    {
      test_objs[i] =  new Tester2(i);
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
      System.out.println("TOTAL TIME TAKEN:::")  ;
      System.out.println(time_taken);
      
    }catch(Exception e)
    {
      e.printStackTrace();
    }
    
    System.out.println("Test done");
  }

}

class TestObject {
  public volatile int index;
  public volatile String  connection_string ; 
  public volatile Connection conn = null ;
  public TestObject(int index)
  {
    this.index = index;
  }
  TestObject(int index, String connection_string)
  {
    this.index = index;
    this.connection_string = connection_string;
    
    try{
      this.conn = DriverManager.getConnection(connection_string,
      "user1", "user1pass");
      conn.setAutoCommit(false);
      System.out.printf("Created the connectins for the index %d \n",this.index);
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
  public volatile String ExtendedInsertStatementParser = "INSERT INTO test_table VALUES (?, 'Prepared_Statement', %d )" ; //INSERT
  public volatile String ExtendedUpdateStatementParser = "UPDATE test_table SET test_name = 'Edited' where thread_id = ? " ;     //UPDATE
  public volatile String ExtendedSelectStatementParser = "Select * from test_table  where id = ?" ;               //SELECT
  public volatile String ExtendedDeleteStatementParser = "Delete from test_table  where id = ?" ;               //DELETE

  /*  Test Function */
  public boolean TestExtendedQuery()
  {
  
    PreparedStatement ppstmtSelect  = null ;
    PreparedStatement ppstmtInsert  = null ;
    PreparedStatement ppstmtDelete  = null ;
    PreparedStatement ppstmtUpdate  = null ;
    Statement stmt = null;
    
    /*  Prepare the prepareStatements */

    try{
      ppstmtSelect  =conn.prepareStatement(this.ExtendedSelectStatementParser); 
      ppstmtInsert  =conn.prepareStatement(String.format(this.ExtendedInsertStatementParser, 1)); 
      ppstmtDelete  =conn.prepareStatement(this.ExtendedDeleteStatementParser); 
      ppstmtUpdate  =conn.prepareStatement(this.ExtendedUpdateStatementParser); 
      stmt =  conn.createStatement();

    }catch(Exception e)
    {
      e.printStackTrace();
      return false ;
    }
    Random rand = new Random();
    int times = loopSize;
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
      int rand_int = rand.nextInt(9);
      int val =times+200*this.index ;
      switch(rand_int) {
        case 0 : 
          /*  Call an insert  Prepared Statement */
          try{
            ppstmtInsert.setInt(1,val );
            int rs = ppstmtInsert.executeUpdate();
            //conn.commit();
          }catch (Exception e)
          {
            e.printStackTrace();
            System.out.println("Called from insert Query");
            return false ;
          }
          break ; 
        case 1 :
          /*  Call an Select  Prepared Statement */
          try{
    
            ppstmtSelect.setInt(1,val-200);
            ResultSet rs = ppstmtSelect.executeQuery();
            //conn.commit();
          }catch (Exception e)
          {
            System.out.println("Called from select Query");
            e.printStackTrace();
            return false ;
          }
          break ;
        case 2 :
          /*  Call an Delete  Prepared Statement */
          try{
            ppstmtDelete.setInt(1,val-200);
             int rs = ppstmtDelete.executeUpdate();
             //conn.commit();
          }catch (Exception e)
          {
            System.out.println("Called from delete Query");
            e.printStackTrace();
            return false ;
          }
          break ;
        case 3 :
          /*  Call an Update  Prepared Statement */
          try{
            ppstmtUpdate.setInt(1,val-200 );
            int rs = ppstmtUpdate.executeUpdate();
            //conn.commit();
          }catch (Exception e)
          {
            System.out.println("Called from update Query");
            e.printStackTrace();
            return false ;
          } 
          break ;
        case 4 : 
          /* Call Insert using simple query */
          try{
         //   stmt.execute(String.format(this.SimpleInsertStatementParser,val, this.index));
          }catch (Exception e)
          {
            e.printStackTrace();
            return false ;
          }
          break ;
        default : 

          break;
      }
    }
    return true ; 
  }
}

class Tester2 implements Runnable {
  public volatile int index;
  public volatile TestObject test_obj ;
  public Tester2( int index )
  {
    //Set the index
    this.index = index; 
    this.test_obj =  new TestObject(this.index, "jdbc:postgresql://localhost:5400/db1");
    
  }

  public void run(){
    
    /*  Create the object */
    System.out.println( this.test_obj.TestExtendedQuery()) ;

  }

}

