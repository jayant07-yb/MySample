#! /bin/sh
mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="help"

PgBouncerConnectionURL="jdbc:postgresql://10.150.4.254:5400/yugabyte"
OdysseyConnectionURL="jdbc:postgresql://10.150.1.213:5400/yugabyte"

NumberOfThreads="1 5 10 50 100 300 500 700 1000"
Tests="WRITE READ"
CommitFrequency=10
LoopSize=500
UserName="yugabyte"
Password="yugabyte"

# Performance testing
echo "--------------------Testing for PgBouncer------------------"
##  PgBouncer
for testname in $Tests
do
  for numberThreads in $NumberOfThreads
    do
      mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="$PgBouncerConnectionURL $UserName $Password $testname $numberThreads $CommitFrequency $LoopSize"
  echo "---------------------------------------------------"
  done
done


##  Odyssey
echo "--------------------Testing for Odyssey------------------"

for testname in $Tests
do
  for numberThreads in $NumberOfThreads
    do
      mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="$OdysseyConnectionURL $username $password $testname $numberThreads $CommitFrequency $LoopSize"
   echo "---------------------------------------------------"
  done
done


# Latency testing
