package eu.pepot.eu.spark.inputsplitter.common

import java.io.{FileNotFoundException, File}

import org.specs2.mutable._
import org.apache.hadoop.fs.Path

class FilesMatcherSpec extends Specification {

  "FilesMatcher" should {

    "match files based on their size" in {
      val smallFile = fileDetails("src/test/resources/files/small.txt")
      val bigFile = fileDetails("src/test/resources/files/big.txt")
      val files = FileDetailsSet(Seq(bigFile, smallFile))

      val FileDetailsSet(matches) = FilesMatcher.matches(files, Condition(biggerThan = Some(50)))

      matches.size mustEqual 1
      matches.head.size mustEqual bigFile.size
      matches.head.path mustEqual bigFile.path
    }

    "match files based on their name" in {
      val smallFile = fileDetails("src/test/resources/files/small.txt")
      val bigFile = fileDetails("src/test/resources/files/big.txt")
      val files = FileDetailsSet(Seq(bigFile, smallFile))

      val FileDetailsSet(matches) = FilesMatcher.matches(files, Condition(namePattern = Some(".*all.*")))

      matches.size mustEqual 1
      matches.head.size mustEqual smallFile.size
      matches.head.path mustEqual smallFile.path
    }

  }

  def fileDetails(path: String): FileDetails = {
    val p = new Path(path)
    val f = new File(path)
    if (!f.exists()) {
      throw new FileNotFoundException(f.getAbsolutePath)
    }
    FileDetails(p, f.length())
  }
}

