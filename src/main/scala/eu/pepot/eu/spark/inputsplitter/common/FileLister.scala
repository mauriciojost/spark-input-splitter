package eu.pepot.eu.spark.inputsplitter.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.mutable

object FileLister {

  def listAllFiles(completeDirectory: String)(implicit sc: SparkContext): FileDetailsSet = {
    val allCompleteFilesIterator = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(completeDirectory), true)
    val files = mutable.ArrayBuffer[FileDetails]()
    while (allCompleteFilesIterator.hasNext) {
      val completeFile = allCompleteFilesIterator.next()
      val fileDetails = FileDetails(completeFile.getPath, completeFile.getLen)
      files += fileDetails
    }

    FileDetailsSet(
      files = files.toSeq
    )

  }

}
