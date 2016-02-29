package eu.pepot.eu.spark.inputsplitter.examples

import eu.pepot.eu.spark.inputsplitter.SplitWriter
import eu.pepot.eu.spark.inputsplitter.common.Condition
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object ExampleSplitWrite {

  val input = "src/test/resources/inputs"
  val splits = "data/splits"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Example")
    sparkConf.setMaster("local[1]")
    sparkConf.registerKryoClasses(
      Array(
        Class.forName("org.apache.hadoop.io.LongWritable"),
        Class.forName("org.apache.hadoop.io.Text")
      )
    );

    implicit val sc = new SparkContext(sparkConf)

    val condition = Condition(biggerThan = Some(50))
    val splitter = new SplitWriter(condition)

    type K = Text
    type V = Text
    type I = KeyValueTextInputFormat
    type O = TextOutputFormat[K, V]

    splitter.writeNewAPI[K, V, I, O](input, splits)

  }

}


