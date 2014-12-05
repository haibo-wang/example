#!/bin/sh
mvn compile
export HADOOP_CLASSPATH=target/classes/
echo $HADOOP_CLASSPATH
hadoop $*
