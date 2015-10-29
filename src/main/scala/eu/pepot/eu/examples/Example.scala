package eu.pepot.eu.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  def main(args: Array[String]) {

    if (args.length != 2) {
      sys.error("Expecting inputDirectory outputDirectory")
    }

    val inputDirectory = args(0)
    val outputDirectory = args(1)

    val conf = new SparkConf()
      .setAppName("XPathFinderSpark")
      .setMaster("local[2]")

    implicit val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(inputDirectory)

    val csvs = lines.map { line =>
      line.split(",")
    }

    val groups = csvs.groupBy { csv =>
      csv(0)
    }

    val latest = groups.map { group =>
      val values = group._2.toList

      val first = values.map(value => value(1)).max

      values.map { value =>
        if (value(1) == first) {
          value ++ "Y"
        } else {
          value ++ "N"
        }
      }
    }

    latest.saveAsTextFile(outputDirectory)

  }

}

