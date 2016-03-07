package eu.pepot.eu.spark.inputsplitter.common.file

case class Mappings(
  bigsToSplits: Set[(FileDetails, FileDetails)]
)

