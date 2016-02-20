package eu.pepot.eu.spark

import eu.pepot.eu.spark.inputsplitter.common._
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SplitReader(
  condition: Condition
) {

  /**
    *
    * @param completeDirectory
    * @param splitsDirectory
    * @param sc
    * @tparam K
    * @tparam V
    * @tparam I
    * @tparam O
    */
  def splitRead[
  K: ClassTag,
  V: ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  O <: OutputFormat[K, V] : ClassTag
  ](
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): Unit = {
    val cuttableRecords: RDD[(K, V)] = selectiveSplitRDD(completeDirectory, splitsDirectory)
    cuttableRecords.saveAsNewAPIHadoopFile[O](splitsDirectory)
  }

  def selectiveSplitRDD[
  O <: OutputFormat[K, V] : ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    completeDirectory: String,
    splitsDirectory: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {
    val allFiles = FileLister.listAllFiles(completeDirectory)
    val splittedFiles = FilesMatcher.matches(allFiles, condition)
    val alreadyOkayFiles = FilesSubstractor.substract(allFiles, splittedFiles)

    sc.newAPIHadoopFile[K, V, I](alreadyOkayFiles.toStringList())
  }


}
