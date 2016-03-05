package eu.pepot.eu.spark.inputsplitter.common.file

import org.slf4j.LoggerFactory

object FileDetailsSetSubstractor {

  val logger = LoggerFactory.getLogger(this.getClass)

  def substract(baseSet: FileDetailsSet, substractSet: FileDetailsSet): FileDetailsSet = {
    FileDetailsSet(
      files = baseSet.files.diff(substractSet.files)
    )
  }

}



