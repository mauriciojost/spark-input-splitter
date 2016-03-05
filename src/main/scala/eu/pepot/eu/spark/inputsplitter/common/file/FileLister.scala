package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.spark.SparkContext

object FileLister {

  def listFiles(directory: String)(implicit sc: SparkContext): FileDetailsSet = {
    val files = org.apache.hadoop.mapreduce.lib.input.FileLister.parseOnlyFiles(directory)
    FileDetailsSet(files.map(f => FileDetails(f.getPath, f.getLen)).toSet)
  }

}
