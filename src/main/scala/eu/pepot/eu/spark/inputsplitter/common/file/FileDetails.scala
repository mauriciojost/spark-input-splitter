package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.hadoop.fs.Path

case class FileDetails(
  path: String,
  size: Long
) {
  def asPath(): Path ={
    new Path(path)
  }
}

