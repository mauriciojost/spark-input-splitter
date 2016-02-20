package eu.pepot.eu.spark.inputsplitter.examples

import eu.pepot.eu.spark.SplitSaver
import eu.pepot.eu.spark.inputsplitter.common.Condition
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object ExampleSplitSave {

  val input = "src/test/resources/files"
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
    val splitter = new SplitSaver(condition)

    type K = Text
    type V = Text
    type I = KeyValueTextInputFormat
    type O = TextOutputFormat[K, V]

    splitter.selectiveSplitSave[K, V, I, O](input, splits)

  }

}


