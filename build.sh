#!/bin/bash

mvn clean compile assembly:single

rm ../spark/elasticsearch-spark-connector.jar

cp ./target/trueno-elasticsearch-spark-connector.jar ../spark/elasticsearch-spark-connector.jar