package eu.pepot.eu.spark.inputsplitter.common.file

case class FileDetailsSet(
  files: Set[FileDetails]
) {
  def toStringList() = files.map(_.path).mkString(",")
}

object FileDetailsSet {
  def toStringList(a: FileDetailsSet, b: FileDetailsSet) = {
    (a.files ++ b.files).map(_.path).mkString(",")
  }
}