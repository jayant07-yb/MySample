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

/*
 * There will be multiple Clients each creating an `UNIQUE` connection to the PGBOUNCER.
 * PGbouncer Config:
 *  MODE - Transaction
 *  RESET_QUERY - SELECT 1
 *  CONNECTION POOL - 5
 * NO OF THREADS :- 100
 */

public class TestPhase1{
  public static void reset_db(){
    try {
        try{
          Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
          System.err.println(e.getMessage());

        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/db1",
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
    
    int n = 100; // Number of connections
    // Create all the prepared statements     
    // Create the objects 
    Tester test_objs[] = new Tester[n];
    for(int i=0;i<n;i++)
    {
      test_objs[i] =  new Tester(i);
    }    

    
    System.out.println("Created Test Objects");

    Thread threads[] = new Thread[n];
    for(int i =0;i<n;i++)
    threads[i] =  new Thread(test_objs[i]);

    try { 
        for(int i =0;i<n;i++)
            threads[i].start();

        for(int i =0;i<n;i++)
            threads[i].join();

    }catch(Exception e)
    {
      e.printStackTrace();
    }
    
    System.out.println("Test done");
  }

}

class Tester implements Runnable {
  public volatile int index;
  
  public Tester( int index)
  {
    this.index = index; 
  }

  public void run(){
    /*
     * 1. Make Connection
     * 2. Make the Prepare Statement
     * 3. Execute it in the loop and make sure all of it runs
     */
    int val =0,loopSize =1000 ;
    try{
    Connection    conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/db1","user1", "user1pass" );      
    conn.setAutoCommit(false); 
    PreparedStatement ppstmt  =conn.prepareStatement(String.format("INSERT INTO test_table VALUES (?, 'Prepared_Statement', %d )", this.index));  
    Statement stmt = conn.createStatement();
   
    String SQL = "SELECT 1";
    //Thread.sleep(1000);
    System.out.printf("Created the connections for the index %d\n",this.index);

    for(int times=0;times<loopSize;times++)
    {  
        int vals = index+(10000*times) ;
        ppstmt.setInt(1,vals); 
        ppstmt.executeUpdate(); 

          val++;

       // ResultSet rs = stmt.executeQuery(SQL);

       // if (!rs.next()) {
        //  System.out.println("Getting NULL in response to SELECT 1");
        //}
        ///if(stmt.execute(String.format("INSERT INTO test_table VALUES (%d, 'Prepared_Statement', %d )",vals,index)))
         // {
          //  System.out.println("Error!!!!!");
          //}
        if(times %2 == 1)
        {conn.commit();
          
          //System.out.printf("Commit Called!!! %d\n" ,  index);
          
        }
    }
    conn.commit();
    ppstmt.close();
    conn.close();

    }catch(Exception e)
    {
      e.printStackTrace();
        System.out.printf("Exception caught %s for the index ::: %d\n",e,index);
     //   System.exit(0);
    }
    System.out.printf("Total success %d out of %d trails\n" , val,loopSize);
  }
}

