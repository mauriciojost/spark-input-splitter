package eu.pepot.eu.spark.inputsplitter.helper

import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait CustomSparkContext extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    registerKryoClasses(
      Array(
        classOf[org.apache.hadoop.io.Text],
        classOf[org.apache.hadoop.io.LongWritable]
      )
    )

  override def beforeAll() {
    _sc = new SparkContext(conf)
    _sc.setLogLevel(org.apache.log4j.Level.WARN.toString)

    super.beforeAll()
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }
}

