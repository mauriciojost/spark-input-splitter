package eu.pepot.eu.spark.inputsplitter.common.file

case class FileDetailsSet(
  files: Set[FileDetails]
) {

  def toStringList() = files.map(_.path).mkString(",")

  def toStringListWith(another: FileDetailsSet) = (files ++ another.files).map(_.path).mkString(",")

}