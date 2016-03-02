package eu.pepot.eu.spark.inputsplitter.common

case class SplitsDir(
  path: String
) {
  def getDataPath = path + "/data"
  def getMetadataPath = path + "/metadata"
}
