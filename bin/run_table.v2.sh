#! /bin/bash

source ./setcp.sh
CLASSPATH=$@,$CLASSPATH

spark-submit --jars $CLASSPATH \
	--class com.cloudera.spark.streaming.v2.StreamingRunnerToTable \
	--master yarn-cluster \
	--executor-cores 4 \
	--executor-memory 16g \
	--num-executors 20 \
	./SparkConsumer.jar $@
