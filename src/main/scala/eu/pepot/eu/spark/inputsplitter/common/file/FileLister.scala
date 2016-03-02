package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object FileLister {

  def listFiles(directory: String)(implicit fs: FileSystem): FileDetailsSet = {
    val allCompleteFilesIterator = fs.listFiles(new Path(directory), true)
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
