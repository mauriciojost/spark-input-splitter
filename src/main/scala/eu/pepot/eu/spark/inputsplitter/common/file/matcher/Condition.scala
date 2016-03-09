package eu.pepot.eu.spark.inputsplitter.common.file.matcher

case class Condition(
  biggerThan: Option[Int] = Some(1024 * 1024 * 128),
  namePattern: Option[String] = None,
  pathCondition: Option[String => Boolean] = None
)

