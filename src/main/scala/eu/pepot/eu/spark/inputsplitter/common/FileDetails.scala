package eu.pepot.eu.spark.inputsplitter.common

import org.apache.hadoop.fs.Path

case class FileDetails(
  path: Path,
  size: Long
)

