# Connectivity Graph
## Compile and run
One possibility is to run ```mvn package```, it may bring some errors at run time depending on the Giraph-Hadoop version you're using. A safe (but dirty) alternative is the following. 
Upload the graph:
```
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal src/main/resources/graph.txt /user/giraph/input/graph.txt
```
Enter the correct folder:
```
cd src/main/java
```
Copy the Giraph examples jar (which contains all the dependencies needed to run the project)
```
cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.3.0-hadoop2-for-hadoop-2.6.5-jar-with-dependencies.jar ./msmcs.jar
```
Compile the code with the proper dependencies
```
javac -cp /usr/local/giraph/giraph-examples/target/giraph-examples-1.3.0-hadoop2-for-hadoop-2.6.5-jar-with-dependencies.jar:$($HADOOP_HOME/bin/hadoop classpath) $(find . -name '*.java')
```
Merge the Giraph examples and the project jars
```
jar uf msmcs.jar it/unitn/msmcs/common/io it/unitn/msmcs/common/messages  it/unitn/msmcs/common/writables it/unitn/msmcs/common/aggregators it/unitn/msmcs/connectivity
```
Remove old Giraph files (if it is not the first run)
```
$HADOOP_HOME/bin/hdfs dfs -rm /user/giraph/aggregatorValues_0
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/giraph/output/connectivity
```
Run the project
```
$HADOOP_HOME/bin/hadoop jar  msmcs.jar org.apache.giraph.GiraphRunner it.unitn.msmcs.connectivity.InitStoerWagner -op /user/giraph/output/connectivity -vof it.unitn.msmcs.common.io.ConnectivityOutputFormat   -mc it.unitn.msmcs.connectivity.ConnectivityMaster -aw org.apache.giraph.aggregators.TextAggregatorWriter -ca giraph.textAggregatorWriter.frequency=1  -eip /user/giraph/input/graph.txt -eif it.unitn.msmcs.common.io.ConnectivityEdgeInputFormat -w 1 -ca giraph.SplitMasterWorker=false,giraph.logLevel=error,giraph.useSuperstepCounters=false,input.directed=false
```
Check the results
```
$HADOOP_HOME/bin/hdfs dfs -text /user/giraph/output/connectivity/p* | less
```

# Maximal SMCS
## Compile and run
The compilation process is similar to the one described for the connectivity graph. 

You merge the jars with the command:
```
jar uf msmcs.jar it/unitn/msmcs/common/io it/unitn/msmcs/maximalSMCS
```

The algorithm expects a custom argument, called ```input.vertices```. This parameter is a "-" separated list of the input vertices IDs. Example:
```
$HADOOP_HOME/bin/hadoop jar  msmcs.jar org.apache.giraph.GiraphRunner it.unitn.msmcs.maximalSMCS.MaximalSMCS -op /user/fabio/output/msmcs -vof it.unitn.msmcs.common.io.SMCSOutputFormat   -eip /user/fabio/input/connectivity.txt -eif it.unitn.msmcs.common.io.SMCSInputFormat -w 1 -ca giraph.SplitMasterWorker=false,giraph.logLevel=error,input.vertices=1-6
```