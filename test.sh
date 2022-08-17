mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="help"
mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.4.254:5400/yugabyte yugabyte yugabyte READ 10 10 10"
mvn -q package exec:java -DskipTests -Dexec.mainClass=com.yugabyte.PerfTest -Dexec.args="jdbc:postgresql://10.150.4.254:5400/yugabyte" -Dexec.args="yugabyte"
