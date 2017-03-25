#Trueno ES Spark Connector Development Guide


### Adding connector dependency
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



## References
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

##Books
* Data Algorithms: Recipes for Scaling Up with Hadoop and Spark
