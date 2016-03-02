package eu.pepot.eu.spark.inputsplitter.common.file.matcher

import org.apache.hadoop.fs.Path

case class Condition(
  biggerThan: Option[Int] = None,
  namePattern: Option[String] = None,
  pathCondition: Option[Path => Boolean] = None
)

