package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.file.{FileLister, FilesSubstractor}
import eu.pepot.eu.spark.inputsplitter.common.splits.{Metadata, SplitDetails, SplitsDir}
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{FunSuite, Matchers}

class SplitReaderSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val inputDir = resourcesBaseDir("scenario-000/input/")
  val splitsDir = resourcesBaseDir("scenario-000/splits/")

  test("the split reader reads correctly the merge of split and smalls (scenario-000)") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // Expecting to have splits of files bigger than 50 bytes

    val inputExpected = FileLister.listFiles(inputDir)(FileSystem.get(scc.hadoopConfiguration))
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)(FileSystem.get(scc.hadoopConfiguration))
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FilesSubstractor.substract(inputExpected, bigsExpected)

    val splitReader = new SplitReader(conditionForSplitting)

    val SplitDetails(rddWithWholeInput, Metadata(splits, bigs, smalls)) = splitReader.rdd[K, V, I, O](inputDir, splitsDir)

    // Tests on inputs
    inputExpected.files.length should be(3)

    // Tests on splits
    splitsExpected.files.length should be(1)
    splits should be (splitsExpected)

    // Tests on bigs
    bigsExpected.files.length should be(1)
    bigs should be (bigsExpected)

    // Tests on smalls
    smallsExpected.files.length should be(2)
    smalls should be (smallsExpected)

    // Tests on the RDD (whole input)
    val expectedRdd = sc.newAPIHadoopFile[K, V, I](inputDir)
    expectedRdd.count() should be (9)
    rddWithWholeInput.count() should be (9)
    expectedRdd.count() should be (rddWithWholeInput.count())
    RDDComparisions.compare(expectedRdd, rddWithWholeInput) should be (None)

  }

}

