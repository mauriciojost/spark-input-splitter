package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class SplitReader(
  condition: Condition
) {

  val logger = LoggerFactory.getLogger(this.getClass)

  def selectiveSplitRDD[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {

    implicit val fs = FileSystem.get(sc.hadoopConfiguration)

    val allFiles = FileLister.listFiles(completeDirectory)
    val eligibleFiles = FilesMatcher.matches(allFiles, condition)
    val splittedFiles = FileLister.listFiles(splitsDirectory)
    val alreadyOkayFiles = FilesSubstractor.substract(allFiles, eligibleFiles)

    sc.newAPIHadoopFile[K, V, I](alreadyOkayFiles.toStringListWith(splittedFiles))

  }


}
