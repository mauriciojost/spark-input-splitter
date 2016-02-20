package eu.pepot.eu.spark.inputsplitter.common

case class Condition(
  biggerThan: Option[Int] = None,
  namePattern: Option[String] = None
)

