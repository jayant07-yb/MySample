package com.yugabyte;

import sun.lwawt.macosx.CSystemTray;

public class MultiDatabaseTest {
    public static void main(String[] args){

        String connection_url = "jdbc:postgresql://localhost:6432/";  //Connection URL
        String username = "yugabyte"; //Username
        String password = "yugabyte"; //Password
        int loopSize =1000;
        int commitFrequency = 1;
        int numberOfThreads = 400;
        long totalTestTime =0;
        TestStructure [] test_obj = new  TestStructure[400];
        for(int obj=0; obj <400 ; obj++)
        {
            test_obj[obj] = new WriteTest(obj, connection_url+String.format("test_db%d",obj%4),username, password , 1000, 1);
        }

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
            System.out.println("Total Query Time:" + newTimeTaken);
            long averageQueryTime = newTimeTaken/(loopSize*numberOfThreads);
            System.out.println("Average time taken"+averageQueryTime);
        }catch(Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }



    }
}

