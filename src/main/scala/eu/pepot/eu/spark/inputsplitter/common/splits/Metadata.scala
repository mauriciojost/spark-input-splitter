package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet}
import org.apache.hadoop.fs.{FileSystem, Path}

case class Metadata(
  splits: FileDetailsSet,
  bigs: FileDetailsSet,
  smalls: FileDetailsSet
)

object Metadata {

  val SPLITS_MAPPING_FILENAME = "splits.mapping"

  def read(splitsDirO: SplitsDir)(implicit fs: FileSystem): Metadata = {
    val splitsMappingFile = new Path(splitsDirO.getMetadataPath + "/" + SPLITS_MAPPING_FILENAME)
    val fsDataInputStream = fs.open(splitsMappingFile)
    val s = scala.io.Source.fromInputStream(fsDataInputStream).getLines().mkString("\n")
    fsDataInputStream.close()
    deserialize(s.getBytes())
    // TODO FIX
  }

  def save(md: Metadata, splitsDirO: SplitsDir)(implicit fs: FileSystem): Unit = {
    val splitsMappingFile = new Path(splitsDirO.getMetadataPath + "/" + SPLITS_MAPPING_FILENAME)
    val fsDataOutputStream = fs.create(splitsMappingFile, true)
    fsDataOutputStream.write(serialize(md))
    fsDataOutputStream.close()
    // TODO FIX
  }

  private def serialize(md: Metadata): Array[Byte] = {
    val bigs = md.bigs.files.map(file => List("I", file.size, file.path.getName))
    val smalls = md.smalls.files.map(file => List("i", file.size, file.path.getName))
    val splits = md.splits.files.map(file => List("s", file.size, file.path.getName))
    val all = bigs ++ smalls ++ splits
    all.map(_.mkString(",")).mkString("\n").getBytes
  }

  private def deserialize(md: Array[Byte]): Metadata = {
    val s = new String(md)
    val lines = s.split("\n").map(line => line.split(","))
    val bigs = lines.filter(_ (0) == "I").map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSeq
    val smalls = lines.filter(_ (0) == "i").map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSeq
    val splits = lines.filter(_ (0) == "s").map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSeq
    Metadata(FileDetailsSet(splits), FileDetailsSet(bigs), FileDetailsSet(smalls))
  }

}
