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

    val inputExpected = FileLister.listFiles(inputDir)
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FileDetailsSetSubstractor.substract(inputExpected, bigsExpected)
    val mappingsExpected = Mappings(
      Set(
        (FileDetails(bigsExpected.files.head.path, 105), FileDetails(splitsExpected.files.head.path, 105))
      )
    )

    val splitWriter = new SplitWriter(Config(conditionForSplitting))

    val SplitDetails(rddWithOnlyBigsRecords, metadata) = splitWriter.asRddNew[K, V, I, O](inputDir)

    // Tests on inputs
    inputExpected.files.size should be(3)

    // Tests on mappings
    //mappingsExpected.bigsToSplits.size should be(1)
    //mappings should be(mappingsExpected)

    // Tests on splits
    splitsExpected.files.size should be(1)
    metadata.splits should be(Set())

    // Tests on bigs
    bigsExpected.files.size should be(1)
    metadata.bigs should be (bigsExpected)

    // Tests on smalls
    smallsExpected.files.size should be(2)
    metadata.smalls should be (smallsExpected)

    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](SplitsDir(splitsDir).getDataPath)
    expectedRddWithOnlyBigFileSplit.count() should be (5)

    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).count() should be (5)
    expectedRddWithOnlyBigFileSplit.count() should be (sc.union(rddWithOnlyBigsRecords.map(_.rdd)).count())
    sc.union(rddWithOnlyBigsRecords.map(_.rdd)).collect() should be (expectedRddWithOnlyBigFileSplit.collect())

  }

}

