package eu.pepot.eu.spark.inputsplitter.common.config

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.Condition

case class Config(
  splitCondition: Condition,
  bytesPerSplit: Float = 1024 * 1024 * 128
) {

  def getAmountOfSplits(bytes: Long): Int = {
    Math.max(2, bytes / bytesPerSplit).toInt
  }

}
