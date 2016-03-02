package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
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
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (splits, smalls, bigs) = determineSplitsSmallsBigs(inputDir, splitsDir)
    val rdd = sc.newAPIHadoopFile[K, V, I](smalls.toStringListWith(splits))
    SplitDetails[K, V](rdd, Some(splits), Some(bigs), Some(smalls))
  }

  private[inputsplitter] def determineSplitsSmallsBigs(
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet, FileDetailsSet) = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    val splits = FileLister.listFiles(splitsDir)
    val smalls = FilesSubstractor.substract(input, bigs)
    (splits, smalls, bigs)
  }
}
