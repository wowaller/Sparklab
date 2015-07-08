#! /bin/bash

source ./setcp.sh
CLASSPATH=$@,$CLASSPATH
#CLASSPATH=$@

spark-submit --jars $CLASSPATH \
	--class com.cloudera.spark.streaming.StreamingHDFSRunner \
	--master yarn-cluster \
	--driver-class-path $@:/opt/cloudera/parcels/CDH/lib/hive/lib/*:/etc/hive/conf \
	--executor-cores 5 \
	--executor-memory 20g \
	--num-executors 16 \
	./SparkConsumer.jar $@
