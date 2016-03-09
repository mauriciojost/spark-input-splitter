package eu.pepot.eu.spark.inputsplitter

import com.holdenkarau.spark.testing.RDDComparisions
import eu.pepot.eu.spark.inputsplitter.common.config.Config
import eu.pepot.eu.spark.inputsplitter.common.file._
import eu.pepot.eu.spark.inputsplitter.common.file.matcher.{Condition, FilesMatcher}
import eu.pepot.eu.spark.inputsplitter.common.splits.resolvers.MetadataResolver
import eu.pepot.eu.spark.inputsplitter.common.splits.{Metadata, SplitDetails, SplitsDir}
import eu.pepot.eu.spark.inputsplitter.helper.CustomSparkContext
import eu.pepot.eu.spark.inputsplitter.helper.TestConstants._
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.{FunSuite, Matchers}

class SplitReaderSpec extends FunSuite with CustomSparkContext with Matchers {

  type K = Text
  type V = Text
  type I = KeyValueTextInputFormat
  type O = TextOutputFormat[K, V]

  val inputDir = resourcesBaseDir("scenario-000/input/")
  val splitsDir = resourcesBaseDir("scenario-000/splits/")

  object CurrdirIdentityMetadataResolver extends MetadataResolver {
    def resolve(metadata: Metadata, discSplits: FileDetailsSet, discBigs: FileDetailsSet, discSmalls: FileDetailsSet): Metadata = {
      def replaceCurrdirS(s: String) = s.replace("/CURRDIR", System.getProperty("user.dir"))
      def replaceCurrdirF(f: FileDetails) = FileDetails(replaceCurrdirS(f.path), f.size)
      def replaceCurrdirFF(f: (FileDetails, FileDetails)): (FileDetails, FileDetails) = (replaceCurrdirF(f._1), replaceCurrdirF(f._2))

      val smalls = FileDetailsSet(metadata.smalls.files.map(replaceCurrdirF))
      val bigs = FileDetailsSet(metadata.bigs.files.map(replaceCurrdirF))
      val mappings = Mappings(metadata.mappings.bigsToSplits.map(replaceCurrdirFF))
      Metadata(mappings, bigs, smalls)
    }
  }

  test("the split reader reads correctly the merge of split and smalls (scenario-000)") {

    implicit val scc = sc

    val conditionForSplitting = Condition(biggerThan = Some(50)) // Expecting to have splits of files bigger than 50 bytes

    val inputExpected = FileLister.listFiles(inputDir)
    val splitsExpected = FileLister.listFiles(SplitsDir(splitsDir).getDataPath)
    val bigsExpected = FilesMatcher.matches(inputExpected, conditionForSplitting)
    val smallsExpected = FileDetailsSetSubstractor.substract(inputExpected, bigsExpected)

    val splitReader = new SplitReader(
      Config(
        splitCondition = conditionForSplitting,
        metadataResolver = CurrdirIdentityMetadataResolver
      )
    )

    val SplitDetails(rddWithWholeInput, metadata) = splitReader.rdds[K, V, I, O](inputDir, splitsDir)

    // TODO mapping asserts

    // Tests on inputs
    inputExpected.files.size should be(3)

    // Tests on splits
    splitsExpected.files.size should be(1)
    metadata.splits should be (splitsExpected.files)

    // Tests on bigs
    bigsExpected.files.size should be(1)
    metadata.bigs.files should be (bigsExpected.files)

    // Tests on smalls
    smallsExpected.files.size should be(2)
    metadata.smalls should be (smallsExpected)

    // Tests on the RDD (whole input)
    val expectedRdd = sc.newAPIHadoopFile[K, V, I](inputDir)
    expectedRdd.count() should be (9)
    sc.union(rddWithWholeInput.map(_.rdd)).count() should be (9)
    expectedRdd.count() should be (sc.union(rddWithWholeInput.map(_.rdd)).count())
    RDDComparisions.compare(expectedRdd, sc.union(rddWithWholeInput.map(_.rdd))) should be (None)

  }

}

