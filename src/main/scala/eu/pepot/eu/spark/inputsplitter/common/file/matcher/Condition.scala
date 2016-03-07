package eu.pepot.eu.spark.inputsplitter.common.file.matcher

case class Condition(
  biggerThan: Option[Int] = None,
  namePattern: Option[String] = None,
  pathCondition: Option[String => Boolean] = None
)

