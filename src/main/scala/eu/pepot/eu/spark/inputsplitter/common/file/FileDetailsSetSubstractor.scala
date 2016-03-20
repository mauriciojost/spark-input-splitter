package eu.pepot.eu.spark.inputsplitter.common.file

import org.apache.log4j.Logger

object FileDetailsSetSubstractor {

  val logger = Logger.getLogger(this.getClass)

  def substract(baseSet: FileDetailsSet, substractSet: FileDetailsSet): FileDetailsSet = {
    FileDetailsSet(
      files = baseSet.files.diff(substractSet.files)
    )
  }

}



