package eu.pepot.eu.spark.inputsplitter.examples

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext, _}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String] + "/" + name
}

object Split {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Split").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("input/path").map { line =>

      val splits = line.split(" ")
      (splits(0), splits(1))

    }

    val rdd2 = rdd.partitionBy(new HashPartitioner(rdd.partitions.length))

    rdd2.saveAsHadoopFile("output/path", classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop()
  }

}


