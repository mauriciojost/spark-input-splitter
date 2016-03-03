package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetailsSet, FileLister, FilesSubstractor}
import eu.pepot.eu.spark.inputsplitter.common.splits.{Metadata, SplitDetails, SplitsDir}
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
    val splitsDirO = SplitsDir(splitsDir)
    val (splits, smalls, bigs) = determineSplitsSmallsBigs(inputDir, splitsDirO)
    val rdd = sc.newAPIHadoopFile[K, V, I](smalls.toStringListWith(splits))
    SplitDetails[K, V](rdd, Metadata(splits, bigs, smalls))
  }

  private[inputsplitter] def determineSplitsSmallsBigs(
    inputDir: String,
    splitsDir: SplitsDir
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet, FileDetailsSet) = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    val splits = FileLister.listFiles(splitsDir.getDataPath)
    val smalls = FilesSubstractor.substract(input, bigs)
    (splits, smalls, bigs)
  }
}
