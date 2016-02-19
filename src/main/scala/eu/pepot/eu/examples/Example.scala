package eu.pepot.eu.examples

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{InputFormat, OutputFormat}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

object Example {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array())

    implicit val sc = new SparkContext(sparkConf)

    val condition = Condition(fileBiggerThanMb = Some(10))
    val tsl = new TopSlicer(condition)

    type K = LongWritable
    type V = Text
    type I = TextInputFormat
    type O = TextOutputFormat[K, V]

    tsl.topSlice[K, V, I, O](classOf[I], classOf[O], classOf[K], classOf[V], "data/completes", "data/cuts")

    //tsl.topSlice("data/completes", "data/cuts") //, condition, LongWritable, Text,  FileOutputFormat[LongWritable, Text], FileInputFormat[LongWritable, Text])(sc)

    //val aa: RDD[MyType] = TopSlicer.fromTopSliced("cuts", "completes")(sc)

  }

}


case class InputFileDetails(path: Path, size: Long)

case class Condition(fileBiggerThanMb: Option[Int])

private class TopSlicer(
  //[K: ClassTag, V: ClassTag, I <: InputFormat[K, V] : ClassTag, O <: OutputFormat[K, V] : ClassTag](
  condition: Condition
) {

  //def foo[T:ClassTag](count: Int, value: T): Array[T] = Array.fill[T](count)(value)

  def topSlice[K: ClassTag, V: ClassTag, I <: InputFormat[K, V] : ClassTag, O <: OutputFormat[K, V] : ClassTag](
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    outputFormatClass: Class[_ <: OutputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    completeDirectory: String,
    cutsDirectory: String
  )(implicit sc: SparkContext): Unit = {

    val files = listAllFiles(completeDirectory)
    files.foreach(println)
    val cuttableFiles = Filter.cuttableFiles(files, condition)
    val listOfFiles = cuttableFiles.map(_.path).mkString(",")
    println(listOfFiles)

    val cuttableRecords = sc.newAPIHadoopFile[K, V, I](listOfFiles)

    cuttableRecords.saveAsTextFile(cutsDirectory)
  }

  def listAllFiles(completeDirectory: String)(implicit sc: SparkContext): Seq[InputFileDetails] = {
    val allCompleteFilesIterator = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(completeDirectory), true)
    val files = mutable.ArrayBuffer[InputFileDetails]()
    while (allCompleteFilesIterator.hasNext) {
      val completeFile = allCompleteFilesIterator.next()
      val fileDetails = InputFileDetails(completeFile.getPath, completeFile.getLen)
      files += fileDetails
    }
    files.toSeq
  }

  //def fromTopSliced[A](completeDirectory: String, cutsDirectory: String)(implicit sc: SparkContext): RDD[A] = { }

}

object Filter {
  def cuttableFiles(files: Seq[InputFileDetails], cutCondition: Condition): Seq[InputFileDetails] = {
    files.filter { currentFile =>
      cutCondition.fileBiggerThanMb.map(minimalFileSize => currentFile.size > minimalFileSize).getOrElse(true)
    }

  }
}
