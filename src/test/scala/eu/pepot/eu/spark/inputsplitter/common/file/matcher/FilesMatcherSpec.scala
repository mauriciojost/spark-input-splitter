package eu.pepot.eu.spark.inputsplitter.common.file.matcher

import java.io.{File, FileNotFoundException}

import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet}
import org.apache.hadoop.fs.Path
import org.specs2.mutable.Specification

class FilesMatcherSpec extends Specification {

  "FilesMatcher" should {

    "match files based on their size" in {
      val smallFile = fileDetails(resourcesBaseDir("scenario-000/input/small1.txt"))
      val bigFile = fileDetails(resourcesBaseDir("scenario-000/input/big.txt"))
      val files = FileDetailsSet(Set(bigFile, smallFile))

      val FileDetailsSet(matches) = FilesMatcher.matches(files, Condition(biggerThan = Some(50)))

      matches.size mustEqual 1
      matches.head mustEqual bigFile
    }

    "match files based on their name" in {
      val smallFile = fileDetails(resourcesBaseDir("scenario-000/input/small1.txt"))
      val bigFile = fileDetails(resourcesBaseDir("scenario-000/input/big.txt"))
      val files = FileDetailsSet(Set(bigFile, smallFile))

      val FileDetailsSet(matches) = FilesMatcher.matches(files, Condition(namePattern = Some(".*all.*")))

      matches.size mustEqual 1
      matches.head mustEqual smallFile
    }

    "match files based on a given path condition" in {
      val smallFile = fileDetails(resourcesBaseDir("scenario-000/input/small1.txt"))
      val bigFile = fileDetails(resourcesBaseDir("scenario-000/input/big.txt"))
      val files = FileDetailsSet(Set(bigFile, smallFile))

      val FileDetailsSet(matches) = FilesMatcher.matches(files, Condition(pathCondition = Some((p: Path) => p.getName().contains("big"))))

      matches.size mustEqual 1
      matches.head mustEqual bigFile
    }

  }

  private def fileDetails(path: String): FileDetails = {
    val p = new Path(path)
    val f = new File(path)
    if (!f.exists()) {
      throw new FileNotFoundException(f.getAbsolutePath)
    }
    FileDetails(p, f.length())
  }

}

