package eu.pepot.eu.spark.inputsplitter.common.config

import eu.pepot.eu.spark.inputsplitter.common.file.matcher.Condition
import eu.pepot.eu.spark.inputsplitter.common.splits.resolvers.{IdentityMetadataResolver, MetadataResolver}

case class Config(
  splitCondition: Condition = Condition(),
  bytesPerSplit: Long = 1024L * 1024 * 128,
  metadataResolver: MetadataResolver = IdentityMetadataResolver,
  rddWriteTimeoutSeconds: Int = 3600
) {

  def getAmountOfSplits(bytes: Long): Int = {
    val splits = bytes / bytesPerSplit
    Math.max(2, splits).toInt
  }

}
