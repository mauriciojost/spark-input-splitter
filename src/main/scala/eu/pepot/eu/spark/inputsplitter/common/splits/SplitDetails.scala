package eu.pepot.eu.spark.inputsplitter.common.splits

import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdds: Seq[RDD[(String, K, V)]],
  metadata: Metadata
)


