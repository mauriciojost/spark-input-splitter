package eu.pepot.eu.examples

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, FileInputFormat, InputFormat, OutputFormat}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Example {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array())

    implicit val sc = new SparkContext(sparkConf)

    val condition = Condition(fileBiggerThanMb = Some(10))
    val tsl = new TopSlicer(condition)

    /*
    implicit val ctK = ClassTag[LongWritable]_
    implicit val ctV = ClassTag[Text]_
    implicit val ctI = ClassTag[FileInputFormat[LongWritable, Text]]_
    */

    sc.hadoopFile[LongWritable, Text]("", classOf[FileInputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text])
    tsl.topSlice[LongWritable, Text, FileInputFormat[LongWritable, Text], FileOutputFormat[LongWritable, Text]](classOf[FileInputFormat[LongWritable, Text]], classOf[FileOutputFormat[LongWritable, Text]], classOf[LongWritable], classOf[Text], "", "")

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

  def topSlice[K, V, I <: InputFormat[K, V], O <: OutputFormat[K, V]](
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    outputFormatClass: Class[_ <: OutputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    completeDirectory: String,
    cutsDirectory: String
  )(implicit sc: SparkContext): Unit = {

    val files = listAllFiles(completeDirectory)
    val cuttableFiles = Filter.cuttableFiles(files, condition)
    val listOfFiles = cuttableFiles.map(_.path.getName).mkString(",")

    val cuttableRecords = sc.hadoopFile[K, V](listOfFiles, inputFormatClass, keyClass, valueClass)

    cuttableRecords.saveAsObjectFile(cutsDirectory)
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
