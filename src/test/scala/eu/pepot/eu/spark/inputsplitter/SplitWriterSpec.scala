package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common._
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{Matchers, FunSuite}

class SplitWriterSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val inputDir = "src/test/resources/inputs"
  val splitsDir = "src/test/resources/splits"

  test("the split writer splits the bigs") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // only big.txt is bigger than this

    val inputExpected = FileLister.listFiles(inputDir)(FileSystem.get(scc.hadoopConfiguration))
    val splitsExpected = FileLister.listFiles(splitsDir)(FileSystem.get(scc.hadoopConfiguration))
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FilesSubstractor.substract(inputExpected, bigsExpected)

    val splitWriter = new SplitWriter(conditionForSplitting)

    val SplitDetails(rddWithOnlyBigsRecords, splits, bigs, smalls) = splitWriter.asRddNew[K, V, I, O](inputDir)

    // Tests on inputs
    inputExpected.files.length should be(3)

    // Tests on splits
    splitsExpected.files.length should be(1)
    splits should be(None)

    // Tests on bigs
    bigsExpected.files.length should be(1)
    bigs should be (Some(bigsExpected))

    // Tests on smalls
    smallsExpected.files.length should be(2)
    smalls should be (Some(smallsExpected))

    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](splitsDir)
    expectedRddWithOnlyBigFileSplit.count() should be (5)

    rddWithOnlyBigsRecords.count() should be (5)
    expectedRddWithOnlyBigFileSplit.count() should be (rddWithOnlyBigsRecords.count())
    RDDComparisions.compare(expectedRddWithOnlyBigFileSplit, rddWithOnlyBigsRecords) should be (None)

  }

}

