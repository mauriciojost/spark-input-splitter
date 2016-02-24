package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.fs.{Path, FileSystem}
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
    val (splittedFiles, notToBeSplittedFiles) = determineSplittables(completeDirectory, splitsDirectory)
    sc.newAPIHadoopFile[K, V, I](notToBeSplittedFiles.toStringListWith(splittedFiles))
  }

  def selectiveSplitRDDWithPath[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): RDD[(Path, K, V)] = {
    val (splittedFiles, notToBeSplittedFiles) = determineSplittables(completeDirectory, splitsDirectory)
    SparkUtils.openWithPath[K, V, I](notToBeSplittedFiles.toStringListWith(splittedFiles))
  }

  private def determineSplittables(
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet) = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    val allFiles = FileLister.listFiles(completeDirectory)
    val eligibleFiles = FilesMatcher.matches(allFiles, condition)
    val splittedFiles = FileLister.listFiles(splitsDirectory)
    val notToBeSplittedFiles = FilesSubstractor.substract(allFiles, eligibleFiles)
    (splittedFiles, notToBeSplittedFiles)
  }
}
