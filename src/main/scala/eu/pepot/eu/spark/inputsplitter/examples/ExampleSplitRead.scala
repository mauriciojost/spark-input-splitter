package eu.pepot.eu.spark.inputsplitter.examples

import eu.pepot.eu.spark.inputsplitter.SplitReader
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object ExampleSplitRead {


  val input = "xtras/scripts/data-sample-00/input"
  val splits = "xtras/scripts/data-sample-00/splits"
  val output = "xtras/scripts/data-sample-00/output"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.setAppName(ExampleSplitRead.getClass.getName)
    sparkConf.setMaster("local[*]")
    sparkConf.registerKryoClasses(
      Array(
        Class.forName("org.apache.hadoop.io.LongWritable"),
        Class.forName("org.apache.hadoop.io.Text")
      )
    );

    implicit val sc = new SparkContext(sparkConf)

    val splitter = new SplitReader()

    type K = Text
    type V = Text
    type I = KeyValueTextInputFormat
    type O = TextOutputFormat[K, V]

    val rdd = splitter.rdd[K, V, I, O](input, splits)

    rdd.saveAsNewAPIHadoopFile[O](output)

    sc.stop()

  }

}


