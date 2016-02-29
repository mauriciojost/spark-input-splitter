package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.Condition
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.FunSuite

class SplitReaderSpec extends FunSuite with CustomSparkContext {

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

    val rddWithWholeInput = splitReader.selectiveSplitRDD[K, V, I, O](input, splits)

    val expected = sc.newAPIHadoopFile[K, V, I](input)

    assert(expected.count() == 9)
    assert(rddWithWholeInput.count() == 9)
    assert(expected.count() == rddWithWholeInput.count())
    assert(None === RDDComparisions.compare(expected, rddWithWholeInput))
  }

  test("the split reader reads correctly the merge of split and smalls (with path)") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // Expecting to have splits of files bigger than 50 bytes

    val splitReader = new SplitReader(conditionForSplitting)

    val rddWithWholeInput = splitReader.selectiveSplitRDDWithPath[K, V, I, O](input, splits)

    val expected = sc.newAPIHadoopFile[K, V, I](input)

    assert(expected.count() == 9)
    assert(rddWithWholeInput.count() == 9)
    assert(expected.count() == rddWithWholeInput.count())
    assert(None === RDDComparisions.compare(expected, rddWithWholeInput.map{case (path, k, v) => (k,v)}))

  }

}

