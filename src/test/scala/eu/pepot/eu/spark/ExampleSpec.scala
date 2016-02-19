package eu.pepot.eu.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class ExampleSpec extends FunSuite with SharedSparkContext {

  test("simple flagging") {
    //assert(None === RDDComparisions.compare(sc.parallelize(expected), Flagger.flagEventsRDD(sc.parallelize(events))(sc)))
  }

}

