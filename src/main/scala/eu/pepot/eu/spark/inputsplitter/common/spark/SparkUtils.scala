package eu.pepot.eu.spark.inputsplitter.common.spark

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD

import scala.reflect.ClassTag

object SparkUtils {

  def openWithPath[K, V, F <: InputFormat[K, V]](input: String)(implicit sc: SparkContext, km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]) = {
    val rawMessages = sc.newAPIHadoopFile[K, V, F](input).asInstanceOf[NewHadoopRDD[K, V]]
    rawMessages mapPartitionsWithInputSplit { case (inputSplit, t) =>
      val path = inputSplit.asInstanceOf[FileSplit].getPath
      t.map { case (k, v) => (path, k, v) }
    }
  }

}
