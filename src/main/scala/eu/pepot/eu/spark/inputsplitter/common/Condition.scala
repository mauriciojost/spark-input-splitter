package eu.pepot.eu.spark.inputsplitter.common

import org.apache.hadoop.fs.Path

case class Condition(
  biggerThan: Option[Int] = None,
  namePattern: Option[String] = None,
  pathCondition: Option[Path => Boolean] = None
)

