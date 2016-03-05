package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetailsSet, FileLister, FileDetailsSetSubstractor}
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
    val discoveredMetadata = determineSplitsSmallsBigs(inputDir, splitsDirO)
    val loadedMetadata = Metadata.load(splitsDirO)(FileSystem.get(sc.hadoopConfiguration))
    val Metadata(resolvedSplits, resolvedBigs, resolvedSmalls) = Metadata.resolve(loadedMetadata, discoveredMetadata)
    val rdd = sc.newAPIHadoopFile[K, V, I](FileDetailsSet.toStringList(resolvedSmalls,resolvedSplits))
    SplitDetails[K, V](rdd, Metadata(resolvedSplits, resolvedBigs, resolvedSmalls))
  }

  private[inputsplitter] def determineSplitsSmallsBigs(
    inputDir: String,
    splitsDir: SplitsDir
  )(implicit sc: SparkContext): Metadata = {
    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    val splits = FileLister.listFiles(splitsDir.getDataPath)
    val smalls = FileDetailsSetSubstractor.substract(input, bigs)
    Metadata(splits, bigs, smalls)
  }
}
