package eu.pepot.eu.spark.inputsplitter.common

import org.apache.hadoop.fs.Path

object FilesMatcher {

  /**
    * Retrieve only the files that conform to the condition.
    * @param files
    * @param condition
    * @return
    */
  def matches(files: Seq[FileDetails], condition: Condition): Seq[FileDetails] = {
    files.filter(currentFile => matchesCondition(currentFile, condition))
  }

  private def matchesCondition(file: FileDetails, condition: Condition): Boolean = {
    condition.biggerThan.map(minimalSize => file.size > minimalSize).getOrElse(false) ||
      condition.namePattern.map(namePattern => file.path.toString.matches(namePattern)).getOrElse(false)
  }
}

case class Condition(
  biggerThan: Option[Int] = None,
  namePattern: Option[String] = None
)

case class FileDetails(
  path: Path,
  size: Long
)


