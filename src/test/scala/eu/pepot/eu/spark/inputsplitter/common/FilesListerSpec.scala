package eu.pepot.eu.spark.inputsplitter.common

import org.specs2.mutable._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration;

class FilesListerSpec extends Specification {

  implicit val fs = FileSystem.getLocal(new Configuration())

  "FileLister" should {

    "list all regular files (1)" in {
      val inputDir = "src/test/resources/inputs"

      val inputs = FileLister.listFiles(inputDir)

      inputs.files.map(_.path.getName).toSet mustEqual
        Set(
          "big.txt",
          "small1.txt",
          "small2.txt"
        )
    }

    "list all regular files (2)" in {
      val inputDir = "src/test/resources/hiddeninputs"

      val inputs = FileLister.listFiles(inputDir)

      inputs.files.map(_.path.getName).toSet mustEqual
        Set(
          "_SUCCESS",
          "part-r-00000"
        )
    }

  }

}
