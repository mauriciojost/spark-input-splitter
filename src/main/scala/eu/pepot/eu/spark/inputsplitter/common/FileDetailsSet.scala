package eu.pepot.eu.spark.inputsplitter.common

case class FileDetailsSet(
  files: Seq[FileDetails]
) {

  def toStringList() = files.map(_.path).mkString(",")

  def toStringListWith(another: FileDetailsSet) = (files ++ another.files).map(_.path).mkString(",")

}