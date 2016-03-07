package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.file.{Mappings, FileDetailsSet, FileDetailsSetSubstractor, FileLister}
import eu.pepot.eu.spark.inputsplitter.common.splits.{Metadata, SplitDetails, SplitsDir}
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
    val splitDetails = asRdd[K, V, I, O](inputDir)
    splitDetails.rdd.map{case (p, k, v) => (k, v)}.saveAsHadoopFile[O](splitsDirO.getDataPath)
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    Metadata.dump(splitDetails.metadata, splitsDirO)
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
    val splitDetails = asRddNew[K, V, I, O](inputDir)
    splitDetails.rdd.map{case (p, k, v) => (k, v)}.saveAsNewAPIHadoopFile[O](splitsDirO.getDataPath)
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    Metadata.dump(splitDetails.metadata, splitsDirO)
  }

  private[inputsplitter] def asRdd[
  K: ClassTag,
  V: ClassTag,
  I <: mapred.InputFormat[K, V] : ClassTag,
  O <: mapred.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (bigs, smalls) = determineBigsSmalls[K, V](inputDir)
    val rdds = bigs.files.map(big => sc.hadoopFile[K, V, I](big.path.toString).map{case (k, v) => (big.path.toString, k, v)})
    val rddRep = sc.union(rdds.toSeq)
    SplitDetails[K, V](rddRep, Metadata(Mappings(Set()), FileDetailsSet(Set()), bigs, smalls))
  }

  private[inputsplitter] def asRddNew[
  K: ClassTag,
  V: ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  O <: mapreduce.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (bigs, smalls) = determineBigsSmalls[K, V](inputDir)
    val rdds = bigs.files.map(_.path.toString).map(f => sc.newAPIHadoopFile[K, V, I](f).map{case (k, v) => (f, k, v)})
    val rddRep: RDD[(String, K, V)] = sc.union(rdds.toSeq)
    SplitDetails[K, V](rddRep, Metadata(Mappings(Set()), FileDetailsSet(Set()), bigs, smalls))
  }

  private def determineBigsSmalls[
  K: ClassTag,
  V: ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet) = {
    val input = FileLister.listFiles(inputDir)
    logger.warn("Using input: {}", inputDir)
    val bigs = FilesMatcher.matches(input, condition)
    logger.warn("Detected bigs from input: {}", bigs)
    val smalls = FileDetailsSetSubstractor.substract(input, bigs)
    logger.warn("Detected smalls from input: {}", smalls)
    (bigs, smalls)
  }

}

