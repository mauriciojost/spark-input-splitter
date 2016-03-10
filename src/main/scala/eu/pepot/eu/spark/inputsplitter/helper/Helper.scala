package eu.pepot.eu.spark.inputsplitter.helper

import org.apache.hadoop.fs.Path

object Helper {

  def toPath(path: String) = new Path(path)

  def toPath(parent: String, child: String) = new Path(parent, child)

  def toStringPath(path: String) = toPath(path).toString

  def toStringPath(parent: String, child: String) = toPath(parent, child).toString

}
