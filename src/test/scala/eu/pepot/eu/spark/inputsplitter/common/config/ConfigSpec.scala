package eu.pepot.eu.spark.inputsplitter.common.config

import org.specs2.mutable._

class ConfigSpec extends Specification{

  val K = 1024L
  val M = 1024 * K

  "Config" should {

    "give correct split numbers" in {
      val config = Config(bytesPerSplit = 100 * K)
      config.getAmountOfSplits(1000 * K) shouldEqual 10
    }

    "give correct split numbers 2" in {
      val config = Config(bytesPerSplit = 100 * K)
      config.getAmountOfSplits(500 * K) shouldEqual 5
    }

    "give correct split numbers 4" in {
      val config = Config()
      config.getAmountOfSplits(627 * M) shouldEqual 4
    }

    "give correct split numbers 5" in {
      val config = Config()
      config.getAmountOfSplits(1) shouldEqual 2 // Minimum
    }

  }
}
