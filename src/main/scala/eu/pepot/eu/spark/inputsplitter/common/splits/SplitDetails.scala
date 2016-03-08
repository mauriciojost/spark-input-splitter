package eu.pepot.eu.spark.inputsplitter.common.splits

case class SplitDetails[K, V](
  arrows: Seq[Arrow[K, V]],
  metadata: Metadata
)


