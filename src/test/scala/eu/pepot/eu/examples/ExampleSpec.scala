package eu.pepot.eu.examples

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class ExampleSpec extends FunSuite with SharedSparkContext {

  test("simple flagging") {
    //assert(None === RDDComparisions.compare(sc.parallelize(expected), Flagger.flagEventsRDD(sc.parallelize(events))(sc)))
  }

}

