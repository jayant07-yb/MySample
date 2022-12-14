#! /bin/sh
mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="help"
#mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.4.254:5400/yugabyte yugabyte yugabyte LATENCY 100"

#mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.4.254:5400/yugabyte yugabyte yugabyte WRITE 100"
#mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.1.213:6432/yugabyte yugabyte yugabyte WRITE 100"
#mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.2.81:5433,10.150.1.32:5433,10.150.2.83:5433/yugabyte yugabyte yugabyte WRITE 10"
 #[debug] execute contextualize
PgBouncerConnectionURL="jdbc:postgresql://10.150.4.254:5400/yugabyte"
OdysseyConnectionURL="jdbc:postgresql://10.150.1.213:6432/yugabyte"
YUGABYTEConnectionURL="jdbc:postgresql://10.150.2.81:5433,10.150.1.32:5433,10.150.2.83:5433/yugabyte"

NumberOfThreads="1 5 10 50 100 200 300 400 500 600 700 800 900 1000"
Tests="WRITE READ"
CommitFrequency=10
LoopSize=500
UserName="yugabyte"
Password="yugabyte"

# Performance testing

##  PgBouncer
for testname in $Tests
do
  for numberThreads in $NumberOfThreads
    do
      #PgBpouncer
        echo "--------------------Testing for PgBouncer------------------"
          sleep 2 #  Sleep for few seconds
          mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="$PgBouncerConnectionURL $UserName $Password $testname $numberThreads $CommitFrequency $LoopSize"

        #PgBpouncer
        #echo "--------------------Testing for Yugabyte------------------"
        #  sleep 2 #  Sleep for few seconds
        #  mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="$YUGABYTEConnectionURL $UserName $Password $testname $numberThreads $CommitFrequency $LoopSize"

  echo "---------------------------------------------------"
  done
done


##  Odyssey
echo "--------------------Testing for Odyssey------------------"

for testname in $Tests
do
  for numberThreads in $NumberOfThreads
    do
      mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="$OdysseyConnectionURL $UserName $Password $testname $numberThreads $CommitFrequency $LoopSize"
   echo "---------------------------------------------------"
  done
done


# Latency testing
