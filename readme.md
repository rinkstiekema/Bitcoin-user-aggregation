#Bitcoin user aggregation

###Usages:
- Spark 
- Hadoop
- Scala
- DataGraph
- Pagerank

###How to run:
First package the program:

`sbt clean assembly`

Then run the program:

`spark-submit --class org.zuinnote.spark.bitcoin.example.SparkScalaBitcoinTransactionGraph --master local[8] target/scala-2.11/example-hcl-spark-scala-graphx-bitcointransaction.jar input-folder-locaion ouput-folder-location`

Then a folder is made, called bitcoin, containing multiple folders.
 - outputVertices (contains the vertices of the datagraph in the form of JSON)
 - outputEdges (contains the edges of the datagraph in the form of JSON)
 - map (contains the mapping from id to the actual bitcoin addresses)
 - count (contains the number of nodes before and after the aggregation)
 - pagerank (contains the edges and vertices, but then rated by the pagerank algorithm)