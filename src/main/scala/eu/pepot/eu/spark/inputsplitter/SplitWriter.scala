package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.{mapred, mapreduce}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class SplitWriter(
  condition: Condition
) {

  val logger = LoggerFactory.getLogger(this.getClass)

  def write[
  K: ClassTag,
  V: ClassTag,
  I <: mapred.InputFormat[K, V] : ClassTag,
  O <: mapred.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): Unit = {
    val splitsDirO = SplitsDir(splitsDir)
    val bigsRecords: RDD[(K, V)] = asRdd[O, I, V, K](inputDir).rdd
    bigsRecords.saveAsHadoopFile[O](splitsDirO.getSplitsPath)
  }

  def writeNewAPI[
  K: ClassTag,
  V: ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  O <: mapreduce.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): Unit = {
    val splitsDirO = SplitsDir(splitsDir)
    val bigsRecords: RDD[(K, V)] = asRddNew[O, I, V, K](inputDir).rdd
    bigsRecords.saveAsNewAPIHadoopFile[O](splitsDirO.getSplitsPath)
  }

  private[inputsplitter] def asRdd[
  O <: mapred.OutputFormat[K, V] : ClassTag,
  I <: mapred.InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (bigs, smalls) = determineBigsSmalls[K, V](inputDir)(sc)
    val rdd = sc.hadoopFile[K, V, I](bigs.toStringList())
    SplitDetails[K, V](rdd, None, Some(bigs), Some(smalls))
  }

  private[inputsplitter] def asRddNew[
  O <: mapreduce.OutputFormat[K, V] : ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (bigs, smalls) = determineBigsSmalls[K, V](inputDir)(sc)
    val rdd = sc.newAPIHadoopFile[K, V, I](bigs.toStringList())
    SplitDetails[K, V](rdd, None, Some(bigs), Some(smalls))
  }

  private def determineBigsSmalls[
  K: ClassTag,
  V: ClassTag
  ](
    inputDir: String
  )(sc: SparkContext): (FileDetailsSet, FileDetailsSet) = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    val input = FileLister.listFiles(inputDir)
    logger.warn("Using input: {}", inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    logger.warn("Detected bigs from input: {}", bigs)
    val smalls = FilesSubstractor.substract(input, bigs)
    logger.warn("Detected smalls from input: {}", smalls)
    (bigs, smalls)
  }

}

