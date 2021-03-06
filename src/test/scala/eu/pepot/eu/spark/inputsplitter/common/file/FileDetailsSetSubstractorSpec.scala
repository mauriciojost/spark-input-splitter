package eu.pepot.eu.spark.inputsplitter.common.file

import eu.pepot.eu.spark.inputsplitter.helper.TestsHelper._
import org.specs2.mutable._

class FileDetailsSetSubstractorSpec extends Specification {

  val big = resourcesBaseDir("scenario-000/input/big.txt")
  val small1 = resourcesBaseDir("scenario-000/input/small1.txt")
  val small2 = resourcesBaseDir("scenario-000/input/small2.txt")
  val split = resourcesBaseDir("scenario-000/splits/data/part-r-00000")

  val small2Abs = resourcesBaseDirWithAbsolutePath("scenario-000/input/small2.txt")

  "FileDetailsSetSubstractorSpec" should {

    "get empty set from identical sets" in {
      val base = FileDetailsSet(Set(toFDs(big), toFDs(small1)))
      val subs = FileDetailsSet(Set(toFDs(big), toFDs(small1)))
      val diff = FileDetailsSetSubstractor.substract(base, subs)
      diff shouldEqual(FileDetailsSet(Set()))
    }

    "get empty set from identical sets in different order" in {
      val base = FileDetailsSet(Set(toFDs(big), toFDs(small1)))
      val subs = FileDetailsSet(Set(toFDs(small1), toFDs(big)))
      val diff = FileDetailsSetSubstractor.substract(base, subs)
      diff shouldEqual(FileDetailsSet(Set()))
    }

    "get non empty set from identical sets (except for absolute paths being used in one)" in {
      val base = FileDetailsSet(Set(toFDs(big), toFDs(small2)))
      val subs = FileDetailsSet(Set(toFDs(big), toFDs(small2Abs)))
      val diff = FileDetailsSetSubstractor.substract(base, subs)
      diff.files.size shouldEqual(1)
    }

    "get difference element when name changes" in {
      val base = FileDetailsSet(Set(FileDetails("a", 10)))
      val subs = FileDetailsSet(Set(FileDetails("b", 10)))
      val diff = FileDetailsSetSubstractor.substract(base, subs)
      diff.files.size shouldEqual(1)
    }

    "get difference element when size changes" in {
      val base = FileDetailsSet(Set(FileDetails("a", 10)))
      val subs = FileDetailsSet(Set(FileDetails("a", 20)))
      val diff = FileDetailsSetSubstractor.substract(base, subs)
      diff.files.size shouldEqual(1)
    }

  }

}
