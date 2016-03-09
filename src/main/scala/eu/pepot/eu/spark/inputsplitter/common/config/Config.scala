package eu.pepot.eu.spark.inputsplitter.common.config

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.Condition
import eu.pepot.eu.spark.inputsplitter.common.splits.resolvers.{IdentityMetadataResolver, MetadataResolver}

case class Config(
  splitCondition: Condition = Condition(),
  bytesPerSplit: Float = 1024 * 1024 * 128,
  metadataResolver: MetadataResolver = IdentityMetadataResolver
) {

  def getAmountOfSplits(bytes: Long): Int = {
    Math.max(2, bytes / bytesPerSplit).toInt
  }

}
