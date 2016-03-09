package eu.pepot.eu.spark.inputsplitter.common.splits.resolvers

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetailsSet
import eu.pepot.eu.spark.inputsplitter.common.splits.Metadata

trait MetadataResolver {
  def resolve(metadata: Metadata, discSplits: FileDetailsSet, discBigs: FileDetailsSet, discSmalls: FileDetailsSet): Metadata
}

