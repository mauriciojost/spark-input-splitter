package eu.pepot.eu.spark

import eu.pepot.eu.spark.inputsplitter.common.{FileDetails, FilesMatcher, Condition}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

class Splitter(
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

  //def fromTopSliced[A](completeDirectory: String, cutsDirectory: String)(implicit sc: SparkContext): RDD[A] = { }

  def selectiveSplitRDD[
  O <: OutputFormat[K, V] : ClassTag,
  I <: InputFormat[K, V] : ClassTag,
  V: ClassTag,
  K: ClassTag
  ](
    completeDirectory: String
  )(implicit sc: SparkContext): RDD[(K, V)] = {
    val files = listAllFiles(completeDirectory)
    val cuttableFiles = FilesMatcher.matches(files, condition)
    val listOfFiles = cuttableFiles.map(_.path).mkString(",")
    sc.newAPIHadoopFile[K, V, I](listOfFiles)
  }

  private def listAllFiles(completeDirectory: String)(implicit sc: SparkContext): Seq[FileDetails] = {
    val allCompleteFilesIterator = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(completeDirectory), true)
    val files = mutable.ArrayBuffer[FileDetails]()
    while (allCompleteFilesIterator.hasNext) {
      val completeFile = allCompleteFilesIterator.next()
      val fileDetails = FileDetails(completeFile.getPath, completeFile.getLen)
      files += fileDetails
    }
    files.toSeq
  }


}
