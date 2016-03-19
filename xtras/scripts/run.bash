#!/bin/bash

CURRDIR=$(readlink -e `dirname $0`)
BASEDIR=$CURRDIR/../../

cd $BASEDIR

export SPARK_HOME=$SPARK_HOME

JAR=$BASEDIR/target/scala-2.10/spark-input-splitter-assembly-0.1.jar

spark-submit --master local-cluster[4,1,1024] --class eu.pepot.eu.spark.inputsplitter.examples.ExampleSplitRead $JAR


