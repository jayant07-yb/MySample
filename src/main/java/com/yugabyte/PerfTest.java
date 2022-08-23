package com.yugabyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Properties;

enum TESTCASE{
    READ,
    WRITE,
    PARSE_ERROR,
    LATENCY,

}

enum INPUT{
    CONNECTION_URL, // The connection url that the test application will use for testing.
    USERNAME,       // The username that the test application will use for testing.
    PASSWORD,       // The password that the test application will use for testing.
    TEST_CASE,      // The type of the test :: "READ" or "WRITE".
    NUMBER_OF_THREADS,  // Number of threads used by the application while testing.
    COMMIT_FREQUENCY,   // Commit will be called after executing how many queries;
                        // Can't be 0;
                        // 1 to set autocommit=true
    LOOP_SIZE,          //Number of queries executed by each thread.
}

public class PerfTest{

    /*  Default Values */

    public static   String connection_url = "jdbc:postgresql://10.150.1.213:6432/postgres";  //Connection URL
    public static   String username = "yugabyte"; //Username
    private static  String password = "yugabyte"; //Password
    public static   TESTCASE testCase = TESTCASE.READ    ; //What is to be tested (SEE TESTCASE ENUM)
    public static int numberOfThreads = 10   ;  //Number of parallel threads that will run the test
    public static int commitFrequency = 2   ;  //Commit will be called after how many queries (1 for autocommit = false ) , Cannot be 0
    public static int loopSize = 1000  ;          //Any thread will execute how many queries

    public static void reset_db(){
        try{
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        try {
            Connection conn = DriverManager.getConnection(connection_url,username, password);
            Statement stmt = conn.createStatement();

            if(testCase != TESTCASE.READ)
                stmt.execute("DROP TABLE IF EXISTS public.test_table");

            stmt.execute("CREATE TABLE IF NOT EXISTS public.test_table" +
                    "  (id decimal primary key, test_name varchar, thread_id int )");

        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static String helpKeyword = "HELP";
    public static void print_help(){
        System.out.println(
        "PerfTest can be used to test the performance of the read and write queries. \n" +
                "Usage::\n" +
                "Input argument to be provided in the sequence:: \n" +
                "    CONNECTION_URL         -- The connection url that the test application will use for testing.\n" +
                "    username               -- The username that the test application will use for testing.\n" +
                "    PASSWORD               -- The password that the test application will use for testing.\n" +
                "    TEST_CASE              -- The type of the test :: \"READ\" or \"WRITE\".\n" +
                "    NUMBER_OF_THREADS      -- Number of threads used by the application while testing.\n" +
                "    COMMIT_FREQUENCY       -- Commit will be called after executing how many queries;\n" +
                "                              Can't be 0;\n" +
                "                              1 to set autocommit=true\n" +
                "    LOOP_SIZE              -- Number of queries executed by each thread."
        );
    }


    /* Output Statistics */
    public static long averageQueryTime;
    public static long totalTestTime;

    public static void main(String[] args) throws ClassNotFoundException, SQLException  {

    /*  Check for help keyword */
        if(args.length != 0 &&  helpKeyword.equals(args[0].toUpperCase()) )
        {
            print_help();
            System.exit(0);
        }

    /*  Load the parameters */
        int currentInput = 0;
        for(String input:args)
        {
            switch (INPUT.values()[ currentInput])
            {
                case CONNECTION_URL:
                    connection_url = input ;
                    break;
                case USERNAME:
                    username = input;
                    break;
                case PASSWORD:
                    password = input;
                    break;
                case TEST_CASE:
                    testCase = TESTCASE.valueOf(input.toUpperCase()) ; //Must be in the format "READ" or "WRITE"
                    break;
                case NUMBER_OF_THREADS:
                    numberOfThreads = Integer.parseInt(input);
                    break;
                case COMMIT_FREQUENCY :
                    commitFrequency = Integer.parseInt(input);
                    break;
                case LOOP_SIZE :
                    loopSize = Integer.parseInt(input);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + testCase);
            }
            currentInput++;
        }

        test(testCase,numberOfThreads,commitFrequency,loopSize);
        print_results();
    }

    public static void print_results()
    {
        System.out.println(
                    "Test parameters\n" +
                    "Connection_url     "   +    connection_url + "\n" +
                    "Username           "   +    username +  "\n" +
                    "Test case          "   +    testCase +  "\n" +
                    "Number of threads  "   +    numberOfThreads +   "\n" +
                    "Commit frequency   "   +    commitFrequency +   "\n" +
                    "Loop Size          "   +    loopSize +  "\n"
        );

        System.out.println(
                    "Test Result\n" +
                    "Average Query Time " + averageQueryTime  + "\n" +
                    "Total Test Time    " + totalTestTime     + "\n"
        );
    }
    public static void test(TESTCASE testCase, int numberOfThreads, int commitFrequency, int loopSize)
    {
        //  Reset the database
        reset_db();

        // Create the test objects
        TestStructure [] test_obj = new  TestStructure[numberOfThreads];

        // Initialize the objects
        // Assign the object
        switch (testCase)
        {
            case READ :
                for(int threadNumber = 0; threadNumber<numberOfThreads; threadNumber++)
                {
                    test_obj[threadNumber] =  new ReadTest( threadNumber, connection_url, username,  password , loopSize , commitFrequency);
                }
                break;
            case WRITE:
                for(int threadNumber = 0; threadNumber<numberOfThreads; threadNumber++)
                {
                    test_obj[threadNumber] =  new WriteTest( threadNumber, connection_url, username,  password , loopSize , commitFrequency);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + testCase);
        }
        System.out.println("Objects created tests");

        //Run the test objects
        Thread threads[] = new Thread[numberOfThreads];
        for(int threadID =0;threadID<numberOfThreads;threadID++)
            threads[threadID] =  new Thread((Runnable) test_obj[threadID]);

        System.out.println("Starting tests");
        try {
            long start = System.currentTimeMillis();
            for(int i =0;i<numberOfThreads;i++)
                threads[i].start();

            for(int i =0;i<numberOfThreads;i++)
                threads[i].join();
            long end = System.currentTimeMillis() ;
            totalTestTime =  (end - start) ;
            long totalrowsprinted=0;
            long newTimeTaken = 0 ;
            for(int i=0;i<numberOfThreads;i++) {
                newTimeTaken = newTimeTaken + (test_obj[i].timeTaken);
                if(testCase == TESTCASE.READ)
                    totalrowsprinted = totalrowsprinted + test_obj[i].NumberOfRowsInReadQueryOutput ;

            }
            System.out.println("Total Query Time:" + newTimeTaken);
            averageQueryTime = newTimeTaken/(loopSize*numberOfThreads);
            if(testCase == TESTCASE.READ)
                System.out.println("Total rows read " + totalrowsprinted);

        }catch(Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }


    }

}

abstract class TestStructure {
    /*  Test Result Parameters */
    public volatile long timeTaken = 0;

    /*  Test Parameters */
    public volatile int loopSize;
    public volatile String  connection_url ;
    public volatile Connection conn = null ;
    public volatile int commit_frequency;

    public TestStructure(int index, String connection_url, String username, String password, int loopSize, int commitFrequency) {
        this.index = index;
        this.connection_url = connection_url;
        this.loopSize = loopSize ;
        this.commit_frequency = commitFrequency;

        try{
            Properties props = new Properties();
            props.setProperty("idle_in_transaction_session_timeout" , "0");
            props.setProperty("password",password);
            props.setProperty("username",username);

            this.conn = DriverManager.getConnection(this.connection_url,props);
            if(commit_frequency <= 1 )
                conn.setAutoCommit(true);
            else {
                conn.setAutoCommit(false);
                conn.commit();
            }

        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    /* Test function */
    public abstract long TestExtendedQuery();
    /*  Extended Statement Parameters */
    public volatile int index;
    public volatile String InsertStatementQueryFormat = "INSERT INTO public.test_table VALUES (?, 'Prepared_Statement', %d )" ; //INSERT
    public volatile String SelectStatementQueryFormat = "Select * from public.test_table  where thread_id = ? LIMIT 10" ;               //SELECT

    public long NumberOfRowsInReadQueryOutput ; //Only for Read test
}

class ReadTest extends TestStructure implements  Runnable{

    public ReadTest(int threadNumber, String connection_url, String username, String password, int loopSize, int commitFrequency) {
        super(threadNumber, connection_url,username, password,loopSize,commitFrequency);
    }

    /*  Test Function */
    public long TestExtendedQuery()
    {
        long start = System.currentTimeMillis() ;

        PreparedStatement preparedStatement_Select = null ;

        /*  Prepare the prepareStatements */
        try{
            preparedStatement_Select =conn.prepareStatement(this.SelectStatementQueryFormat);
        }catch(Exception e)
        {
            e.printStackTrace();
            return 0 ;
        }

        if(this.commit_frequency >  1 )
        {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        NumberOfRowsInReadQueryOutput =0;
        for(int times=0;times<loopSize;times++)
        {
            int SelectIndex = index ;
            try{
                preparedStatement_Select.setInt(1,SelectIndex);
                ResultSet rs2 = preparedStatement_Select.executeQuery();
                while(rs2.next())
                {
                    NumberOfRowsInReadQueryOutput++;
                }
            }catch(Exception e)
            {
                e.printStackTrace();
                System.exit(1);
            }

            if(this.commit_frequency >  1 && times%commit_frequency==0)
            {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        if(this.commit_frequency >  1 )
        {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
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
        return  NumberOfRowsInReadQueryOutput;
    }

    @Override
    public void run() {
        TestExtendedQuery();
    }
}

class WriteTest extends TestStructure implements  Runnable{



    public WriteTest(int threadNumber, String connection_url, String username, String password, int loopSize, int commitFrequency) {
        super(threadNumber, connection_url, username, password, loopSize, commitFrequency);
    }

    /*  Test Function */
    public long TestExtendedQuery()
    {
        long start = System.currentTimeMillis() ;

        PreparedStatement preparedStatement_Insert = null ;

        /*  Prepare the prepareStatements */
        try{
            preparedStatement_Insert =conn.prepareStatement(String.format(this.InsertStatementQueryFormat, index));
        }catch(Exception e)
        {
            e.printStackTrace();
            return 0 ;
        }

        if(this.commit_frequency >  1 )
        {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        long NumberOfRowsInReadQueryOutput =0;
        for(int times=0;times<loopSize;times++)
        {
            long insertID  = times + index*(loopSize+1);
            try{
                preparedStatement_Insert.setLong(1,insertID);
                preparedStatement_Insert.executeUpdate();
            }catch(Exception e)
            {
                e.printStackTrace();
                System.exit(1);
            }

            if(this.commit_frequency >  1 && times%commit_frequency==0)
            {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        if(this.commit_frequency >  1 )
        {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        try{
            conn.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis() ;
        this.timeTaken = end - start ;
        return  1;
    }

    @Override
    public void run() {
        TestExtendedQuery();
    }
}

