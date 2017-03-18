# Trueno Elastic Search Spark Connector

Trueno's ElasticSearch Connector for [Apache Spark](#apache-spark).

## Requirements
Elasticsearch 2.x cluster accessible through [transport][].

## Installation

Available through any Maven-compatible tool:

```xml
<dependency>
  <groupId>org.trueno.elasticsearch.spark.connector</groupId>
  <artifactId>elasticsearch-spark-connector</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Using Trueno's Elastic Search Spark Connector

```
spark-2.1.0-bin-hadoop2.7$ ./bin/spark-shell --jars elasticsearch-spark-connector.jar 
```

## Loading library from scala/spark

```
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.0
      /_/
         
Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_91)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.trueno.elasticsearch.spark.connector._
import org.trueno.elasticsearch.spark.connector._

scala> val transportClient = new ESTransportClient()
transportClient: org.trueno.elasticsearch.spark.connector.ESTransportClient = org.trueno.elasticsearch.spark.connector.ESTransportClient@31b7112d

scala> val verticesRDD = transportClient.getVertexRDD()
Elasticsearch client retrieving vertices ... 
```

## Example library

```scala
/* Trueno ES Spark Connector */
import org.trueno.elasticsearch.spark.connector._

/* GraphX references */
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

val transportClient = new ESTransportClient()

val verticesRDD = transportClient.getVertexRDD()

val edgesRDD = transportClient.getEdgeRDD()

val graph = transportClient.getGraph()

val g2 = PageRank.runUntilConvergence(graph,0.001)

```
