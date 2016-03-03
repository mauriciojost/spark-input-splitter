package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetailsSet, FileLister, FilesSubstractor}
import eu.pepot.eu.spark.inputsplitter.common.splits.{Metadata, SplitDetails, SplitsDir}
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._
import org.apache.hadoop.fs.FileSystem
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

    val inputExpected = FileLister.listFiles(inputDir)(FileSystem.get(scc.hadoopConfiguration))
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)(FileSystem.get(scc.hadoopConfiguration))
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FilesSubstractor.substract(inputExpected, bigsExpected)

    val splitWriter = new SplitWriter(conditionForSplitting)

    val SplitDetails(rddWithOnlyBigsRecords, Metadata(splits, bigs, smalls)) = splitWriter.asRddNew[K, V, I, O](inputDir)

    // Tests on inputs
    inputExpected.files.length should be(3)

    // Tests on splits
    splitsExpected.files.length should be(1)
    splits should be(FileDetailsSet(Nil))

    // Tests on bigs
    bigsExpected.files.length should be(1)
    bigs should be (bigsExpected)

    // Tests on smalls
    smallsExpected.files.length should be(2)
    smalls should be (smallsExpected)

    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](SplitsDir(splitsDir).getDataPath)
    expectedRddWithOnlyBigFileSplit.count() should be (5)

    rddWithOnlyBigsRecords.count() should be (5)
    expectedRddWithOnlyBigFileSplit.count() should be (rddWithOnlyBigsRecords.count())
    RDDComparisions.compare(expectedRddWithOnlyBigFileSplit, rddWithOnlyBigsRecords) should be (None)

  }

}

