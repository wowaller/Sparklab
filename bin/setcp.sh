#!/bin/sh

export CLOUDERA_LIB=/opt/cloudera/parcels/CDH/lib/
#CLASSPATH=/etc/hbase/conf,/etc/zookeeper/conf,/etc/hadoop/conf
CLASSPATH=/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar

for file in `ls $CLOUDERA_LIB/hadoop/client/*.jar`; do
    if [[ -z $CLASSPATH ]]; then 
        CLASSPATH=$file
    else
        CLASSPATH=$CLASSPATH,$file
    fi
done

for file in `ls $CLOUDERA_LIB/hbase/*jar`; do
    CLASSPATH=$CLASSPATH,$file
done

for file in `ls $CLOUDERA_LIB/hbase/lib/*jar`; do
    CLASSPATH=$CLASSPATH,$file
done

for file in `ls $CLOUDERA_LIB/hive/lib/*jar`; do
    CLASSPATH=$CLASSPATH,$file
done

#for file in `ls $CLOUDERA_LIB/llama/*jar`; do
#    CLASSPATH=$CLASSPATH,$file
#done

#for file in `ls $CLOUDERA_LIB/llama/lib/*jar`; do
#    CLASSPATH=$CLASSPATH,$file
#done

#for file in `ls /root/ccbtest/*jar`; do
#    CLASSPATH=$CLASSPATH,$file
#done

#echo $CLASSPATH

export CLASSPATH=$CLASSPATH
#export CLASSPATH=
#alias j=java -cp $CLASSPATH


