package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.Condition
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.FunSuite

class SplitWriterSpec extends FunSuite with CustomSparkContext {

  test("simple flagging") {

    implicit val scc = sc

    val input = "src/test/resources/inputs"
    val splits = "src/test/resources/splits"
    val output = "data/ouptut"

    // TODO change name SplitSaver to SplitWriter
    val conditionForSplitting = Condition(biggerThan = Some(50)) // Same already used in SplitSaver

    val splitWriter = new SplitWriter(conditionForSplitting)

    type K = Text
    type V = Text
    type I = KeyValueTextInputFormat
    type O = TextOutputFormat[K, V]

    val rddWithSplitInput = splitWriter.selectiveSplitRDD[O, I, K, V](input)

    val expected = sc.newAPIHadoopFile[K, V, I](splits)

    assert(expected.count() == 5)
    assert(rddWithSplitInput.count() == 5)
    assert(expected.count() == rddWithSplitInput.count())
    assert(None === RDDComparisions.compare(expected, rddWithSplitInput))
  }

}

