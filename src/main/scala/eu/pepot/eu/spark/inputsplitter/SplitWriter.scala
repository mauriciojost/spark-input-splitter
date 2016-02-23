package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem

import scala.reflect.ClassTag

class SplitWriter(
  condition: Condition
) {

  /**
    *
    * @param completeDirectory
    * @param cutsDirectory
    * @param sc
    * @tparam K
    * @tparam V
    * @tparam I
    * @tparam O
    */
  def selectiveSplitSave[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    completeDirectory: String,
    cutsDirectory: String
  )(implicit sc: SparkContext): Unit = {
    val cuttableRecords: RDD[(K, V)] = selectiveSplitRDD(completeDirectory)
    cuttableRecords.saveAsNewAPIHadoopFile[O](cutsDirectory)
  }

  def selectiveSplitRDD[
  O <: OutputFormat[K, V] : ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    completeDirectory: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {

    implicit val fs = FileSystem.get(sc.hadoopConfiguration)

    val files = FileLister.listFiles(completeDirectory)
    val cuttableFiles = FilesMatcher.matches(files, condition)
    sc.newAPIHadoopFile[K, V, I](cuttableFiles.toStringList())
  }

}
