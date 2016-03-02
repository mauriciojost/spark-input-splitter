package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.hadoop.fs.Path

case class FileDetails(
  path: Path,
  size: Long
)

