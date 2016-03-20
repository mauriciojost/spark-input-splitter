package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetails
import org.apache.spark.rdd.RDD
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

class ArrowSpec extends FunSuite with Matchers with MockitoSugar {

  val K = 1024L
  val M = 1024 * K
  type K = String
  type V = String
  val itsRdd = mock[RDD[(K, V)]]
  val itsPath = "/a/path/to/a/file"

  test("give correct split numbers") {
    val arrow = Arrow[K, V](FileDetails(itsPath, 1000 * K), itsRdd)
    arrow.getNroExpectedSplits(bytesPerSplit = 100 * K) should be(10)
  }

  test("give correct split numbers 2") {
    val arrow = Arrow[K, V](FileDetails(itsPath, 500 * K), itsRdd)
    arrow.getNroExpectedSplits(bytesPerSplit = 100 * K) shouldEqual 5
  }

  test("give correct split numbers 5") {
    val arrow = Arrow[K, V](FileDetails(itsPath, 1 * K), itsRdd)
    arrow.getNroExpectedSplits(bytesPerSplit = 128 * M) shouldEqual 2 // Minimum
  }

}
