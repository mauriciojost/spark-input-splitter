package eu.pepot.eu.spark.inputsplitter.common

import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdd: RDD[(K, V)],
  bigs: FileDetailsSet
)

