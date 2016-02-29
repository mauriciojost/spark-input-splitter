package eu.pepot.eu.spark.inputsplitter.common

case class SplitsDir(
  path: String
) {
  def getSplitsPath = path + "/splits"
  def getMetadata = path + "/metadata"
}
