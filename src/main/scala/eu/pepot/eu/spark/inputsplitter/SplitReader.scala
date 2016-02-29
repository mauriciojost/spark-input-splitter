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

  def rdd[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {
    val (splits, smalls) = determineBigs(inputDir, splitsDir)
    sc.newAPIHadoopFile[K, V, I](smalls.toStringListWith(splits))
  }

  def rddWithPath[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): RDD[(Path, K, V)] = {
    val (splits, smalls) = determineBigs(inputDir, splitsDir)
    SparkUtils.openWithPath[K, V, I](smalls.toStringListWith(splits))
  }

  private[inputsplitter] def determineBigs(
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet) = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    val splits = FileLister.listFiles(splitsDir)
    val smalls = FilesSubstractor.substract(input, bigs)
    (splits, smalls)
  }
}
