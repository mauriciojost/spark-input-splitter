package eu.pepot.eu.spark.inputsplitter.common

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

class FilesMatcherSpec extends FunSuite with SharedSparkContext {

  test("simple size based matching") {

    val smallFile = FileDetails(new Path("src/test/resources/files/small.txt"), 10)
    val bigFile = FileDetails(new Path("src/test/resources/files/big.txt"), 100)
    val files = Seq(bigFile, smallFile)

    val condition = Condition(
      biggerThan = Some(50)
    )

    val matches = FilesMatcher.matches(files, condition)
    assert(matches.size == 1)
    assert(matches(0).size == 100)
  }

}
