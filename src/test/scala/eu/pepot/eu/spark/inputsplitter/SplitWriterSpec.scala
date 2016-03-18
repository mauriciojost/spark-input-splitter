package eu.pepot.eu.spark.inputsplitter

import eu.pepot.eu.spark.inputsplitter.common.config.Config
import eu.pepot.eu.spark.inputsplitter.common.file._
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.splits.{SplitDetails, SplitsDir}
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestsHelper._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{FunSuite, Matchers}

class SplitWriterSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val inputDir = resourcesBaseDir("scenario-000/input/")
  val splitsDir = resourcesBaseDir("scenario-000/splits/")

  test("the split writer splits the bigs (scenario-000)") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // only big.txt is bigger than this

    val splitWriter = new SplitWriter(Config(conditionForSplitting))

    val inputExpected = FileLister.listFiles(inputDir)
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FileDetailsSetSubstractor.substract(inputExpected, bigsExpected)
    //val mappingsExpected = Mappings(Set((toFDs(bigsExpected.files.head.path), toFDs(splitsExpected.files.head.path))))
    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](SplitsDir(splitsDir).getDataPath)

    // Asserts on expected metadata and RDD (sanity check)
    inputExpected.files.size should be(3)
    splitsExpected.files.size should be(1)
    bigsExpected.files.size should be(1)
    smallsExpected.files.size should be(2)
    expectedRddWithOnlyBigFileSplit.count() should be(5)
    //mappingsExpected.bigsToSplits.size should be(1)


    val SplitDetails(rddWithOnlyBigsRecords, metadata) = splitWriter.asRddNew[K, V, I, O](inputDir)


    // Asserts on the metadata returned
    metadata.splits should be(Set())
    metadata.bigs should be(bigsExpected)
    metadata.smalls should be(smallsExpected)
    //metadata.mappings should be(mappingsExpected)

    // Asserts on the RDD (should be equivalent to the bigs)
    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).count() should be(expectedRddWithOnlyBigFileSplit.count())
    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).collect() should be(expectedRddWithOnlyBigFileSplit.collect())

  }

  test("the split writer splits the bigs with good mapping (scenario-000)") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // only big.txt is bigger than this

    val splitWriter = new SplitWriter(Config(conditionForSplitting))

    val inputExpected = FileLister.listFiles(inputDir)
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FileDetailsSetSubstractor.substract(inputExpected, bigsExpected)
    val mappingsExpected = Mappings(Set((toFDs(bigsExpected.files.head.path), toFDs(splitsExpected.files.head.path))))
    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](SplitsDir(splitsDir).getDataPath)

    // Asserts on expected metadata and RDD (sanity check)
    mappingsExpected.bigsToSplits.size should be(1)


    val SplitDetails(rddWithOnlyBigsRecords, metadata) = splitWriter.asRddNew[K, V, I, O](inputDir)


    // Asserts on the metadata returned
    metadata.splits should be(Set())
    metadata.bigs should be(bigsExpected)
    metadata.smalls should be(smallsExpected)
    //metadata.mappings should be(mappingsExpected)

    // Asserts on the RDD (should be equivalent to the bigs)
    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).count() should be(expectedRddWithOnlyBigFileSplit.count())
    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).collect() should be(expectedRddWithOnlyBigFileSplit.collect())

  }

}

