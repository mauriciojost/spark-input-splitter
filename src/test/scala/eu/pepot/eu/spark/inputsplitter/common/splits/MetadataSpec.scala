package eu.pepot.eu.spark.inputsplitter.common.splits

import java.io.File

import com.google.common.io.Files
import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet}
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.{FunSuite, Matchers}
import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._

class MetadataSpec extends FunSuite with CustomSparkContext with Matchers {

  test("the metadata should be serialized and deserialized correctly") {

    val big = resourcesBaseDir("scenario-000/input/big.txt")
    val small1 = resourcesBaseDir("scenario-000/input/small1.txt")
    val small2 = resourcesBaseDir("scenario-000/input/small2.txt")
    val split = resourcesBaseDir("scenario-000/splits/data/part-r-00000")

    def toFileDetails(s: String) = FileDetails(new Path(s), new File(s).length())

    implicit val scc = sc
    implicit val fs = FileSystem.get(scc.hadoopConfiguration)


    val mds = List(
      Metadata(
        splits = FileDetailsSet(Nil),
        bigs = FileDetailsSet(Nil),
        smalls = FileDetailsSet(Nil)
      ),
      Metadata(
        splits = FileDetailsSet(Nil),
        bigs = FileDetailsSet(List(toFileDetails(big))),
        smalls = FileDetailsSet(List(toFileDetails(small1)))
      ),
      Metadata(
        splits = FileDetailsSet(List(toFileDetails(split))),
        bigs = FileDetailsSet(List(toFileDetails(big))),
        smalls = FileDetailsSet(List(toFileDetails(small1), toFileDetails(small2)))
      )
    )

    mds.foreach { md =>
      val testTmpDir = Files.createTempDir()

      val splitsDir = SplitsDir(testTmpDir.getAbsolutePath)
      Metadata.dump(md, splitsDir)
      val loaded = Metadata.load(splitsDir)
      loaded should be(md)

      FileUtils.deleteDirectory(testTmpDir)
    }
  }

}
