package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.helper.Helper
import Helper._

case class SplitsDir(
  path: String
) {
  def getDataPath = toStringPath(path, "data")

  def getMetadataPath = toStringPath(path, "metadata")

  def getDataPathWith(suffix: String) = toStringPath(toStringPath(path, "data"), suffix)
}
