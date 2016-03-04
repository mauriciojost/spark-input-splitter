package eu.pepot.eu.spark.inputsplitter.common.splits

import com.google.common.io.Files
import eu.pepot.eu.spark.inputsplitter.common.file.FileDetailsSet
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{FunSuite, Matchers}

class MetadataSpec extends FunSuite with CustomSparkContext with Matchers {

  val big = resourcesBaseDir("scenario-000/input/big.txt")
  val small1 = resourcesBaseDir("scenario-000/input/small1.txt")
  val small2 = resourcesBaseDir("scenario-000/input/small2.txt")
  val split = resourcesBaseDir("scenario-000/splits/data/part-r-00000")

  test("the metadata should be serialized and deserialized correctly") {

    implicit val scc = sc
    implicit val fs = FileSystem.get(scc.hadoopConfiguration)

    val mds = List(
      Metadata(
        splits = FileDetailsSet(Set()),
        bigs = FileDetailsSet(Set()),
        smalls = FileDetailsSet(Set())
      ),
      Metadata(
        splits = FileDetailsSet(Set()),
        bigs = FileDetailsSet(Set(toFDs(big))),
        smalls = FileDetailsSet(Set(toFDs(small1)))
      ),
      Metadata(
        splits = FileDetailsSet(Set(toFDs(split))),
        bigs = FileDetailsSet(Set(toFDs(big))),
        smalls = FileDetailsSet(Set(toFDs(small1), toFDs(small2)))
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

  test("metadata should resolve loaded and discovered metadatas using discovered one") {

    implicit val scc = sc
    implicit val fs = FileSystem.get(scc.hadoopConfiguration)

    val mdLoaded = Metadata(
      splits = FileDetailsSet(Set(toFDs(split))),
      bigs = FileDetailsSet(Set(toFDs(big))),
      smalls = FileDetailsSet(Set(toFDs(small1), toFDs(small2)))
    )
    val mdDiscovered = Metadata(
      splits = FileDetailsSet(Set()),
      bigs = FileDetailsSet(Set(toFDs(big))),
      smalls = FileDetailsSet(Set(toFDs(small1)))
    )

    val mdResolved = Metadata.resolve(mdLoaded, mdDiscovered)

    mdResolved should be(mdDiscovered)

  }

  /*
  TODO: The scenario to support is as follows:

  1. First the user generate splits from files called:

      big   + small

  2. Splits generated are:

      (big) + small + big1 + big2           (real == reported) == I(big)+i(small)+s(big1,big2)

  3. Next, after generation, a new file is written (that was not there during initial generation)

      (big) + small + big1 + big2 + XXX     (real != reported) => reported as above, while real is I(big,XXX)+i(small)+s(big1,big2)

  4. The user requests a split reading.
  What should be processed is what has been reported + all files that are not in the report as smalls

  */

}
