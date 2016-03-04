package eu.pepot.eu.spark.inputsplitter.common.file

object FileDetailsSetSubstractor {

  def substract(baseSet: FileDetailsSet, substractSet: FileDetailsSet): FileDetailsSet = {
    FileDetailsSet(
      files = baseSet.files.diff(substractSet.files)
    )
  }

}



