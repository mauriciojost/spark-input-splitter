package eu.pepot.eu.spark.inputsplitter.common.splits

case class SplitsDir(
  path: String
) {
  def getDataPath = path + "/data"
  def getMetadataPath = path + "/metadata"

  def getDataPathWith(suffix: String) = path + "/data/" + suffix
}
