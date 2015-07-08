#!/bin/bash

source ./setcp.sh
export CLASSPATH=$CLASSPATH:./SparkConsumer.jar

echo $CLASSPATH

java -cp $CLASSPATH com.cloudera.kafka.example.KafkaProducerExample $@
