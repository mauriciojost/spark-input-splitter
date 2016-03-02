package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common._
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
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

  val inputDir = "src/test/resources/inputs/"
  val splitsDir = "src/test/resources/splits/"

  test("the split reader reads correctly the merge of split and smalls") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // Expecting to have splits of files bigger than 50 bytes

    val inputExpected = FileLister.listFiles(inputDir)(FileSystem.get(scc.hadoopConfiguration))
    val splitsExpected = FileLister.listFiles(splitsDir)(FileSystem.get(scc.hadoopConfiguration))
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FilesSubstractor.substract(inputExpected, bigsExpected)


    val splitReader = new SplitReader(conditionForSplitting)

    val SplitDetails(rddWithWholeInput, splits, bigs, smalls) = splitReader.rdd[K, V, I, O](inputDir, splitsDir)

    // Tests on splits
    splits should be (Some(splitsExpected))

    // Tests on bigs
    bigs should be (Some(bigsExpected))

    // Tests on smalls
    smalls should be (Some(smallsExpected))

    // Tests on the RDD (whole input)
    val expectedRdd = sc.newAPIHadoopFile[K, V, I](inputDir)
    expectedRdd.count() should be (9)
    rddWithWholeInput.count() should be (9)
    expectedRdd.count() should be (rddWithWholeInput.count())
    RDDComparisions.compare(expectedRdd, rddWithWholeInput) should be (None)

  }

}

