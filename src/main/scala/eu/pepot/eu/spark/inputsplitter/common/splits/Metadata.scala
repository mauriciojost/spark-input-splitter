package eu.pepot.eu.spark.inputsplitter.common.splits

import java.io.ObjectOutputStream

import eu.pepot.eu.spark.inputsplitter.common.file.FileDetailsSet
import org.apache.hadoop.fs.{FileSystem, Path}

case class Metadata(
  splits: Option[FileDetailsSet],
  bigs: Option[FileDetailsSet],
  smalls: Option[FileDetailsSet]
)

object Metadata {

  val SPLITS_MAPPING_FILENAME = "splits.mapping"

  def save(md: Metadata, splitsDirO: SplitsDir)(implicit fs: FileSystem): Unit = {
    val splitsMappingFile = new Path(splitsDirO.getMetadataPath + "/" + SPLITS_MAPPING_FILENAME)
    val fsDataOutputStream = fs.create(splitsMappingFile, true)
    val oos = new ObjectOutputStream(fsDataOutputStream)
    oos.writeObject(md)
    fsDataOutputStream.close()
    // TODO FIX
  }

}
