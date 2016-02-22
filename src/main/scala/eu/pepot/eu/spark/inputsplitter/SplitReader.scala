package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SplitReader(
  condition: Condition
) {

  def selectiveSplitRDD[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {
    val allFiles = FileLister.listNonHiddenFiles(completeDirectory)
    val eligibleFiles = FilesMatcher.matches(allFiles, condition)
    val splittedFiles = FileLister.listNonHiddenFiles(splitsDirectory)
    val alreadyOkayFiles = FilesSubstractor.substract(allFiles, eligibleFiles)

    sc.newAPIHadoopFile[K, V, I](alreadyOkayFiles.toStringListWith(splittedFiles))

  }


}
