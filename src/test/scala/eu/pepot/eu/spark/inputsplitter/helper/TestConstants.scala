package eu.pepot.eu.spark.inputsplitter.helper

import java.io.File

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetails
import org.apache.hadoop.fs.Path

object TestConstants {

  def resourcesBaseDir(subDir: String) = {
    "src/test/resources/eu/pepot/eu/spark/inputsplitter/samples/" + subDir
  }

  def resourcesBaseDirWithAbsolutePath(subDir: String) = {
    new File("src/test/resources/eu/pepot/eu/spark/inputsplitter/samples/" + subDir).getAbsolutePath
  }

  def toFDs(s: String): FileDetails = {
    FileDetails(new Path(s), new File(s).length())
  }


}
