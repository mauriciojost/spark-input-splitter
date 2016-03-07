package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetails
import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdds: Seq[(FileDetails, RDD[(K, V)])],
  metadata: Metadata
)


