package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.Condition
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{Matchers, FunSuite}

class SplitReaderSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val input = "src/test/resources/inputs"
  val splits = "src/test/resources/splits"

  test("the split reader reads correctly the merge of split and smalls") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // Expecting to have splits of files bigger than 50 bytes

    val splitReader = new SplitReader(conditionForSplitting)

    val rddWithWholeInput = splitReader.rdd[K, V, I, O](input, splits).rdd

    val expected = sc.newAPIHadoopFile[K, V, I](input)

    expected.count() should be (9)
    rddWithWholeInput.count() should be (9)
    expected.count() should be (rddWithWholeInput.count())
    RDDComparisions.compare(expected, rddWithWholeInput) should be (None)
    // TODO complete tests taking into account SplitDetails
  }

}

