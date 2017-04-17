# Trueno's ElasticSearch Spark Connector Development Guide


### Including connector dependency
```scala
import trueno.elasticsearch.spark.connector._
```

### Spark dependencies
```scala
import org.apache.spark.SparkContext    
import org.apache.spark.SparkContext._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD.fromRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
```

### GraphX Dependencies
```scala
/* GraphX references */
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.rdd.RDD
```

### Scala and Java collections
```scala
import java.util.{Map => JMap}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
```

### Instantiating Trueno's ElasticSearch and Spark connector
```scala
val tc = new ESTransportClient(index, sc)
```
* **index** refers to the ElasticSearch Index. The cluster is set by default to *localhost*.
* **sc** SparkContext

### Retrieving Vertices from the ElasticSearch Cluster
```scala
val verticesESJavaRDD = tc.getLongVertexRDD()
```
* Retrieves a **JavaRDD[Long]** with **all** Vertices
  
  Spark-shell result
  ```text
     Retrieving vertices JavaRDD[Long] ... 
     Index biogrid
     Retrieved Vertices [316719]
     verticesESJavaRDD: org.apache.spark.api.java.JavaRDD[Long] = ParallelCollectionRDD[3] at parallelize at ESTransportClient.java:201
  ```

### Converting JavaRDD[Long] to RDD[Long]
```scala
val verticesESRDD = verticesESJavaRDD.rdd
```
* Converts **JavaRDD** to **RDD**
  
  Spark-Shell Result
    ```text
    scala> val verticesESRDD = verticesESJavaRDD.rdd
    verticesESRDD: org.apache.spark.rdd.RDD[Long] = ParallelCollectionRDD[3] at parallelize at ESTransportClient.java
    ```

### Retrieving Edges from ElasticSearch Cluster
```scala
val edgeJavaRDD = tc.getEdgeRDDHashMap()
```

### Converting JavaRDD to RDD
```scala
val esRDD = scalaFakeRDD.rdd
```
* Converts from **JavaRDD** to **RDD**

  **Spark-Shell** result
    ```text
       esRDD: org.apache.spark.rdd.RDD[scala.collection.mutable.Map[Long,Long]] = ParallelCollectionRDD[1] at parallelize at ESTransportClient.java:375
    ```

### Generating EdgeRDD from the retrieved ElasticSearch RDD
```scala
val edgeRDD: RDD[Edge[Long]] = esRDD.flatMap( x=> ( x.map (y => ( Edge(y._1, y._2, 1.toLong) ) ) ) )
```

  **Spark-Shell** result
  
```text
   edgeRDD: org.apache.spark.rdd.RDD[org.apache.spark.graphx.Edge[Long]] = MapPartitionsRDD[2] at flatMap at <console>:60
```







### References
* https://github.com/holdenk/learning-spark-examples/blob/master/src/main/java/com/oreilly/learningsparkexamples/java/logs/LogAnalyzerWindowed.java
* https://github.com/elastic/elasticsearch-hadoop/blob/master/spark/core/main/scala/org/elasticsearch/spark/rdd/api/java/JavaEsSpark.scala
* https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
* https://github.com/holdenk/elasticsearchspark/blob/master/src/main/scala/com/holdenkarau/esspark/IndexTweetsLive.scala
* https://spark.apache.org/docs/2.0.0/api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html
* https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/api/java/JavaRDD.html
* https://stackoverflow.com/questions/11903167/convert-java-util-hashmap-to-scala-collection-immutable-map-in-java
* https://stackoverflow.com/questions/16918956/convert-java-map-to-scala-map
* http://docs.scala-lang.org/overviews/collections/maps.html
* http://www.scala-lang.org/old/node/1193.html
* http://alvinalexander.com/scala/how-to-traverse-map-for-loop-foreach-scala-cookbook
* http://alvinalexander.com/scala/create-iterating-scala-string-maps
* http://docs.scala-lang.org/overviews/collections/overview.html
* https://stackoverflow.com/questions/20556009/scala-iterate-over-map-and-turn-singleton-list-into-just-the-singleton
* https://stackoverflow.com/questions/6364468/how-to-iterate-scala-map
* https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch11s18.html
* https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch11s19.html
* http://www.scala-lang.org/api/2.12.x/scala/collection/mutable/Map.html
* http://www.scala-lang.org/api/2.12.0/scala/collection/Iterable.html
* https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/EdgeRDD.scala
* https://gist.github.com/ceteri/c2a692b5161b23d92ed1
* http://stackoverflow.com/questions/19598369/cant-instantiate-map-well-why-not
* http://stackoverflow.com/questions/9058070/how-to-convert-a-mutable-hashmap-into-an-immutable-equivalent-in-scala
* http://stackoverflow.com/questions/33112727/get-values-from-an-rdd
* http://stackoverflow.com/questions/28111455/how-to-transform-rddkey-value-into-mapkey-rddvalue
* http://stackoverflow.com/questions/3993613/what-is-the-syntax-for-adding-an-element-to-a-scala-collection-mutable-map
* https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html
* http://alvinalexander.com/scala/converting-java-collections-to-scala-list-map-array
* http://stackoverflow.com/questions/31628605/how-to-convert-a-hashmap-to-a-javapairrdd-in-spark
* http://stackoverflow.com/questions/21495117/using-scala-map-in-java
* http://softwareengineering.stackexchange.com/questions/203464/how-do-i-initialize-a-scala-map-with-more-than-4-initial-elements-in-java
* http://alvinalexander.com/scala/scala-maps-map-class-examples
* https://www.cs.helsinki.fi/u/wikla/OTS/Sisalto/examples/html/ch17.html
* http://www.cs.sjsu.edu/~pearce/modules/lectures/scala/maps/index.htm
* https://github.com/SHSE/spark-es/blob/master/src/main/scala/org/apache/spark/elasticsearch/ElasticSearchRDD.scala
* [Ankur] http://apache-spark-user-list.1001560.n3.nabble.com/noob-how-to-extract-different-members-of-a-VertexRDD-td12399.html
* http://stackoverflow.com/questions/32080708/how-to-convert-a-map-to-sparks-rdd
* https://stackoverflow.com/questions/32396477/how-to-create-a-graph-from-a-csv-file-using-graph-fromedgetuples-in-spark-scala
* https://stackoverflow.com/questions/33892240/large-task-size-for-simplest-program


### Issues
* https://github.com/graphframes/graphframes/issues/116
* https://stackoverflow.com/questions/32396477/how-to-create-a-graph-from-a-csv-file-using-graph-fromedgetuples-in-spark-scala

### Building Spark
*  https://github.com/apache/spark

```text
build/mvn -DskipTests clean package
```

### Books
* Data Algorithms: Recipes for Scaling Up with Hadoop and Spark
* GraphX in Action
