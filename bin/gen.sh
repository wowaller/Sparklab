#! /bin/bash

source ./setcp.sh

spark-submit --jars $CLASSPATH \
	--class com.cloudera.graph.kshell.TestGraphGen \
	--master yarn-client \
	--executor-memory 512M \
	--num-executors 8 \
	--driver-java-options \
	-Dspark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/* \
	./SparkOnHBase.jar $@
