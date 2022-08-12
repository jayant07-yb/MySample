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

public class PrepareQueryPerformanceTest{
  public static void reset_db(){
    try {
        try{
          Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
          System.err.println(e.getMessage());

        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte",
                                                      "yugabyte", "yugabyte");
        Statement stmt = conn.createStatement();

        stmt.execute("DROP TABLE IF EXISTS public.test_table");
        
        stmt.execute("CREATE TABLE IF NOT EXISTS public.test_table" +
                    "  (id decimal primary key, test_name varchar, thread_id int )");
        stmt.execute("grant all on test_table to user1");
    
                  } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(1);
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
    
        Tester6 test_objs[] = new Tester6[n];
        for(int i=0;i<n;i++)
        {
          test_objs[i] =  new Tester6(i);
        }    
    
        
       // System.out.println("Created Test Objects");
    
        Thread threads[] = new Thread[n];
        for(int i =0;i<n;i++)
        threads[i] =  new Thread(test_objs[i]);
        System.out.println("Starting tests");
        try { 
            long start = System.currentTimeMillis();
            for(int i =0;i<n;i++)
                threads[i].start();
    
            for(int i =0;i<n;i++)
                threads[i].join();
            long end = System.currentTimeMillis() ;
            long time_taken =  (end - start) ;
            //return time_taken;
            
            long newTimeTaken = 0 ;
            for(int i=0;i<n;i++)
              newTimeTaken = newTimeTaken + (test_objs[i].timeTaken ); 
           // newTimeTaken = newTimeTaken; 
            return newTimeTaken/(500*n);
            
    
        }catch(Exception e)
        {
          e.printStackTrace();
        }
        return 0;
  }

}

class TestObject6 {  
  public volatile long timeTaken = 0;

  public volatile int index;
  public volatile String  connection_string ; 
  public volatile Connection conn = null ;
  public TestObject6(int index)
  {
    this.index = index;
  }
  TestObject6(int index, String connection_string)
  {
    this.index = index;
    this.connection_string = connection_string;
    
    try{
      this.conn = DriverManager.getConnection(this.connection_string,"user1", "user1pass"); 
      //conn.setAutoCommit(false);
      //System.out.printf("Created the connectins for the index %d \n",this.index);
    }catch(Exception e)
    {
      e.printStackTrace();
    }
  }
  

  /*  Test Parameters */
  public volatile int loopSize = 500;
  public volatile String ExtendedInsertStatementParser = "INSERT INTO public.test_table VALUES (?, 'Prepared_Statement', %d )" ; //INSERT
  public volatile String ExtendedUpdateStatementParser = "UPDATE public.test_table SET test_name = 'Edited' where thread_id = ? " ;     //UPDATE
  public volatile String ExtendedSelectStatementParser = "Select * from public.test_table  where id = ?" ;               //SELECT
  public volatile String ExtendedDeleteStatementParser = "Delete from public.test_table  where id = ?" ;               //DELETE

  /*  Test Function */
  public boolean TestExtendedQuery()
  {
    long start = System.currentTimeMillis() ;

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
      //conn.commit();
    }catch(Exception e)
    {
      e.printStackTrace();
      return false ;
    }
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
      int rand_int = 1; //rand.nextInt(2);
      int val =times+(loopSize)*this.index ;
      try{
      switch (rand_int) {
        case 1:
        ppstmtInsert.setInt(1,val );
        int rs = ppstmtInsert.executeUpdate();            
        break;
        case 0:
        ppstmtSelect.setInt(1,val-loopSize);
        ResultSet rs2 = ppstmtSelect.executeQuery();        
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
    conn.close();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
    long end = System.currentTimeMillis() ;
    this.timeTaken = end - start ; 
    return true ; 
  }
}

class Tester6 implements Runnable {
  public volatile int index;
  public volatile long timeTaken; 
  public volatile TestObject6 test_obj ;
  public Tester6( int index )
  {
    //Set the index
    this.index = index; 
    this.test_obj =  new TestObject6(this.index, "jdbc:postgresql://localhost:5400/yugabyte");
  }

  public void run(){  
    /*  Create the object */
    this.test_obj.TestExtendedQuery();
    this.timeTaken = test_obj.timeTaken ; 

  }
}

