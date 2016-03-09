package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common.config.Config
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.FilesMatcher
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetailsSetSubstractor, FileLister}
import eu.pepot.eu.spark.inputsplitter.common.splits.{Arrow, Metadata, SplitDetails, SplitsDir}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class SplitReader(
  config: Config
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
    val (discSplits, discBigs, discSmalls) = determineSplitsSmallsBigs(inputDir, splitsDirO)
    val loadedMetadata = Metadata.load(splitsDirO)(FileSystem.get(sc.hadoopConfiguration))
    val resolvedMetadata = loadedMetadata.resolve(discSplits, discBigs, discSmalls)(config.metadataResolver)
    val rdds = resolvedMetadata.smallsAndSplits.map(f => Arrow(f, sc.newAPIHadoopFile[K, V, I](f.path)))
    SplitDetails[K, V](rdds.toSeq, resolvedMetadata)
  }

  private[inputsplitter] def determineSplitsSmallsBigs(
    inputDir: String,
    splitsDir: SplitsDir
  )(implicit sc: SparkContext) = {
    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, config.splitCondition)
    val splits = FileLister.listFiles(splitsDir.getDataPath)
    val smalls = FileDetailsSetSubstractor.substract(input, bigs)
    (splits, bigs, smalls)
  }
}
