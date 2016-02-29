package eu.pepot.eu.spark.inputsplitter.common

import org.apache.spark.rdd.RDD

case class SplitDetails[K, V](
  rdd: RDD[(K, V)],
  splits: Option[FileDetailsSet],
  bigs: Option[FileDetailsSet],
  smalls: Option[FileDetailsSet]
)

