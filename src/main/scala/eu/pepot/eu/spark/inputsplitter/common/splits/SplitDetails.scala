package eu.pepot.eu.spark.inputsplitter.common.splits

import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdds: Seq[(String, RDD[(K, V)])],
  metadata: Metadata
)


