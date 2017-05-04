#!/bin/bash

mvn clean compile assembly:single

rm ../spark/elasticsearch-spark-connector.jar

rm ../scala/es_spark_conn_test/lib/elasticsearch-spark-connector.jar

cp ./target/trueno-elasticsearch-spark-connector.jar ../scala/es_spark_conn_test/lib/elasticsearch-spark-connector.jar