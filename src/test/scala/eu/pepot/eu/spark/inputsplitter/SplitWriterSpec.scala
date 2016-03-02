package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.Condition
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{Matchers, FunSuite}

class SplitWriterSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val input = "src/test/resources/inputs"
  val splits = "src/test/resources/splits"

  test("the split writer splits the bigs") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // only big.txt is bigger than this

    val splitWriter = new SplitWriter(conditionForSplitting)

    val rddWithOnlyBigsRecords = splitWriter.asRddNew[O, I, K, V](input).rdd

    val expectedRddWithOnlyBigFileSplit = sc.newAPIHadoopFile[K, V, I](splits)

    expectedRddWithOnlyBigFileSplit.count() should be (5)
    rddWithOnlyBigsRecords.count() should be (5)
    expectedRddWithOnlyBigFileSplit.count() should be (rddWithOnlyBigsRecords.count())
    RDDComparisions.compare(expectedRddWithOnlyBigFileSplit, rddWithOnlyBigsRecords) should be (None)
  }

}

