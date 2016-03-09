package eu.pepot.eu.spark.inputsplitter.common.splits.resolvers

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetailsSet
import eu.pepot.eu.spark.inputsplitter.common.splits.Metadata

object IdentityMetadataResolver extends MetadataResolver {
  def resolve(metadata: Metadata, discSplits: FileDetailsSet, discBigs: FileDetailsSet, discSmalls: FileDetailsSet): Metadata = {
    metadata
  }
}

