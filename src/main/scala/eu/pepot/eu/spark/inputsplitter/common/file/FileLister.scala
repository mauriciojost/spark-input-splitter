package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

object FileLister {

  val logger = LoggerFactory.getLogger(this.getClass)

  def listFiles(directory: String)(implicit sc: SparkContext): FileDetailsSet = {
    val files = org.apache.hadoop.mapreduce.lib.input.FileLister.parseOnlyFiles(directory)
    FileDetailsSet(files.map(f => FileDetails(f.getPath.toString, f.getLen)).toSet)
  }

}
