package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapred
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

  def writeNew[
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
  )(implicit sc: SparkContext): Split[K, V] = {

    implicit val fs = FileSystem.get(sc.hadoopConfiguration)

    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)

    logger.warn("Using input: {}", inputDir)
    logger.warn("Detected splittables: {}", bigs)

    val rdd = sc.hadoopFile[K, V, I](bigs.toStringList())
    Split[K, V](rdd, bigs)
  }

  private[inputsplitter] def asRddNew[
  O <: mapreduce.OutputFormat[K, V] : ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): Split[K, V] = {

    implicit val fs = FileSystem.get(sc.hadoopConfiguration)

    val input = FileLister.listFiles(inputDir)
    val bigs = FilesMatcher.matches(input, condition)

    logger.warn("Using input: {}", inputDir)
    logger.warn("Detected bigs: {}", bigs)

    val rdd = sc.newAPIHadoopFile[K, V, I](bigs.toStringList())
    Split[K, V](rdd, bigs)
  }

}

case class Split[K, V](
  rdd: RDD[(K, V)],
  bigs: FileDetailsSet
)

