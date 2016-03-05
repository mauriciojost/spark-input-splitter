package eu.pepot.eu.spark.inputsplitter.common.file.matcher

import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet}
import org.slf4j.LoggerFactory

object FilesMatcher {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Retrieve only the files that conform to the condition.
    * @param files
    * @param condition
    * @return
    */
  def matches(files: FileDetailsSet, condition: Condition): FileDetailsSet = {
    FileDetailsSet(
      files = files.files.filter(currentFile => matchesCondition(currentFile, condition))
    )
  }

  private def matchesCondition(file: FileDetails, condition: Condition): Boolean = {
    condition.biggerThan.map(minimalSize => file.size > minimalSize).getOrElse(false) ||
      condition.namePattern.map(namePattern => file.path.toString.matches(namePattern)).getOrElse(false) ||
        condition.pathCondition.map(f => f(file.path)).getOrElse(false)
  }

}


