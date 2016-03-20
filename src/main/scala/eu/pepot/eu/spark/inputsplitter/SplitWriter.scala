package eu.pepot.eu.spark.inputsplitter

import java.util.concurrent.{Executors, TimeUnit}

import eu.pepot.eu.spark.inputsplitter.common.config.Config
import eu.pepot.eu.spark.inputsplitter.common.file._
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.FilesMatcher
import eu.pepot.eu.spark.inputsplitter.common.splits.{Arrow, Metadata, SplitDetails, SplitsDir}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce
import org.apache.spark.SparkContext
import org.apache.log4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

class SplitWriter(
  config: Config = Config()
) {
  val rddWriteTimeoutSeconds = Duration(config.rddWriteTimeoutSeconds, TimeUnit.SECONDS)
  val tp = Executors.newCachedThreadPool()
  implicit val ec = ExecutionContext.fromExecutorService(tp)

  val logger = Logger.getLogger(this.getClass)

  def writeNewAPI[
  K: ClassTag,
  V: ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  O <: mapreduce.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String,
    splitsDir: String
  )(implicit sc: SparkContext): Unit = {
    val splitsDirO = SplitsDir(splitsDir)
    val splitDetails = asRddNew[K, V, I, O](inputDir)
    val futureResults = splitDetails.arrows.map { arrow =>
      val outputDirectory = splitsDirO.getDataPathWith(arrow.big.asPath().getName)
      val result = arrow.rdd.repartition(arrow.getNroExpectedSplits(config.bytesPerSplit))
      Future(result.saveAsNewAPIHadoopFile[O](outputDirectory))
    }

    waitForFutures(futureResults)

    val mappings = splitDetails.arrows.flatMap { arrow =>
      val outputDirectory = splitsDirO.getDataPathWith(arrow.big.asPath().getName)
      val outputPartitionFiles = FileLister.listFiles(outputDirectory).files
      outputPartitionFiles.map(outputPartitionFile => (arrow.big, outputPartitionFile))
    }.toSet
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    Metadata.dump(Metadata(Mappings(mappings), splitDetails.metadata.bigs, splitDetails.metadata.smalls), splitsDirO)
  }

  def waitForFutures(futureResults: Seq[Future[Unit]]): Unit = {
    futureResults.foreach { f =>
      Await.result(f, rddWriteTimeoutSeconds)
      f.onFailure {
        case failure => throw failure
      }
    }
  }

  private[inputsplitter] def asRddNew[
  K: ClassTag,
  V: ClassTag,
  I <: mapreduce.InputFormat[K, V] : ClassTag,
  O <: mapreduce.OutputFormat[K, V] : ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): SplitDetails[K, V] = {
    val (bigs, smalls) = determineBigsSmalls[K, V](inputDir)
    val rdds = bigs.files.map(f => Arrow(f, sc.newAPIHadoopFile[K, V, I](f.path)))
    SplitDetails[K, V](rdds.toSeq, Metadata(Mappings(Set()), bigs, smalls))
  }

  private def determineBigsSmalls[
  K: ClassTag,
  V: ClassTag
  ](
    inputDir: String
  )(implicit sc: SparkContext): (FileDetailsSet, FileDetailsSet) = {
    val input = FileLister.listFiles(inputDir)
    logger.info("Using input: " + inputDir)
    val bigs = FilesMatcher.matches(input, config.splitCondition)
    logger.info("Detected bigs from input: " + bigs)
    val smalls = FileDetailsSetSubstractor.substract(input, bigs)
    logger.info("Detected smalls from input: " + smalls)
    (bigs, smalls)
  }

}

