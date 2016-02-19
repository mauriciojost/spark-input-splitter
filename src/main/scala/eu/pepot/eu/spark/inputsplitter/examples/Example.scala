package eu.pepot.eu.spark.inputsplitter.examples

import eu.pepot.eu.spark.Splitter
import eu.pepot.eu.spark.inputsplitter.common.Condition
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array())

    implicit val sc = new SparkContext(sparkConf)

    val condition = Condition(biggerThan = Some(10))
    val splitter = new Splitter(condition)

    type K = LongWritable
    type V = Text
    type I = TextInputFormat
    type O = TextOutputFormat[K, V]

    splitter.selectiveSplit[K, V, I, O](classOf[I], classOf[O], classOf[K], classOf[V], "data/completes", "data/cuts")

  }

}


