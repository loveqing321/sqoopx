#!/bin/bash

CURRENT_DIR=$(cd `dirname $0`; pwd)

LIB=$CURRENT_DIR/../lib

CLASSPATH=$CLASSPATH:$CURRENT_DIR/../conf

jarList=$(ls $CURRENT_DIR/../lib | grep jar)

for i in $jarList
do
   CLASSPATH=$CLASSPATH:"$LIB/$i"
done

export CLASSPATH=$CLASSPATH

CLASS=com.deppon.hadoop.sqoopx.core.SqoopxMain

jobId=job01

exec java -Dsqoopx.launcher.job.id=$jobId $CLASS $@