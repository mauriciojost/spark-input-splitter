package eu.pepot.eu.spark

import eu.pepot.eu.spark.inputsplitter.common.{FileDetails, FilesMatcher, Condition}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.reflect.ClassTag

private class Splitter(
  condition: Condition
) {

  /**
    *
    * @param inputFormatClass
    * @param outputFormatClass
    * @param keyClass
    * @param valueClass
    * @param completeDirectory
    * @param cutsDirectory
    * @param sc
    * @tparam K
    * @tparam V
    * @tparam I
    * @tparam O
    */
  def selectiveSplit[K: ClassTag, V: ClassTag, I <: InputFormat[K, V] : ClassTag, O <: OutputFormat[K, V] : ClassTag](
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    outputFormatClass: Class[_ <: OutputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    completeDirectory: String,
    cutsDirectory: String
  )(implicit sc: SparkContext): Unit = {

    val files = listAllFiles(completeDirectory)
    val cuttableFiles = FilesMatcher.matches(files, condition)
    val listOfFiles = cuttableFiles.map(_.path).mkString(",")

    val cuttableRecords = sc.newAPIHadoopFile[K, V, I](listOfFiles)

    cuttableRecords.saveAsTextFile(cutsDirectory)

  }

  //def fromTopSliced[A](completeDirectory: String, cutsDirectory: String)(implicit sc: SparkContext): RDD[A] = { }

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
