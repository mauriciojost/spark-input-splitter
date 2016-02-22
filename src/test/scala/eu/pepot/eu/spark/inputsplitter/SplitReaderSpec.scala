package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.Condition
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.FunSuite

class SplitReaderSpec extends FunSuite with CustomSparkContext {

  test("simple flagging") {

    implicit val scc = sc

    val input = "src/test/resources/inputs"
    val splits = "src/test/resources/splits"
    val output = "data/ouptut"

    // TODO change name SplitSaver to SplitWriter
    val conditionForSplitting = Condition(biggerThan = Some(50)) // Same already used in SplitSaver

    val splitReader = new SplitReader(conditionForSplitting)

    type K = Text
    type V = Text
    type I = KeyValueTextInputFormat
    type O = TextOutputFormat[K, V]

    val rddWithWholeInput = splitReader.selectiveSplitRDD[K, V, I, O](input, splits)

    val expected = sc.newAPIHadoopFile[K, V, I](input)

    assert(expected.count() == 6)
    assert(rddWithWholeInput.count() == 6)
    assert(expected.count() == rddWithWholeInput.count())
    assert(None === RDDComparisions.compare(expected, rddWithWholeInput))
  }

}

