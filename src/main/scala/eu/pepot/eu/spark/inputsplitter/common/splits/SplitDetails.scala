package eu.pepot.eu.spark.inputsplitter.common.splits

import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdd: RDD[(K, V)],
  metadata: Metadata
)


