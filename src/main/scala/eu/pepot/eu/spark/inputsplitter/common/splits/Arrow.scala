package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetails
import org.apache.spark.rdd.RDD

case class Arrow[K, V](
  big: FileDetails,
  rdd: RDD[(K, V)]
)
