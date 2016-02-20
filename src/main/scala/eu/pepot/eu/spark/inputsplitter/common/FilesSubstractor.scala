package eu.pepot.eu.spark.inputsplitter.common

object FilesSubstractor {

  def substract(files: FileDetailsSet, substract: FileDetailsSet): FileDetailsSet = {
    FileDetailsSet(
      files = files.files.diff(substract.files)
    )
  }

}



