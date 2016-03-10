package eu.pepot.eu.spark.inputsplitter.common.file

import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestsHelper._
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.scalatest.{FunSuite, Matchers}

class FilesListerSpec extends FunSuite with CustomSparkContext with Matchers {

  test("FileLister must list all regular files (1)") {

    implicit val scc = sc

    val inputDir = resourcesBaseDir("scenario-000/input")
    val inputs = FileLister.listFiles(inputDir)
    inputs.files.map(_.asPath.getName).toSet should be(Set("big.txt", "small1.txt", "small2.txt"))

  }

  test("list all regular files (2)") {

    implicit val scc = sc

    val inputDir = resourcesBaseDir("hidden")
    val inputs = FileLister.listFiles(inputDir)
    inputs.files.map(_.asPath.getName).toSet should be(Set("part-r-00000"))

  }

  test("throw exception if bad path") {

    implicit val scc = sc

    val inputDir = resourcesBaseDir("scenario-000/input-inexistent")
    val thrown = intercept[Exception] {
      FileLister.listFiles(inputDir)
    }
    assert(thrown.isInstanceOf[InvalidInputException])

  }


}

