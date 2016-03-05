package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.slf4j.LoggerFactory

case class Metadata(
  splits: FileDetailsSet,
  bigs: FileDetailsSet,
  smalls: FileDetailsSet
)

object Metadata {

  val logger = LoggerFactory.getLogger(this.getClass)

  val BIG_KEY = "I"
  val SMALL_KEY = "i"
  val SPLIT_KEY = "s"
  val ROW_SEPARATOR = ","
  val LINE_SEPARATOR = "\n"

  val SPLITS_MAPPING_FILENAME = "splits.mapping"

  def resolve(loadedMetadata: Metadata, discoveredMetadata: Metadata) = {
    // TODO Saved metadata is ignored for now.
    loadedMetadata match {
      case `discoveredMetadata` => {
        logger.debug("Matching metadatas: " + loadedMetadata)
        discoveredMetadata
      }
      case _ => {
        logger.warn("Non matching metadatas, loaded:\n" + loadedMetadata + "\n,discovered:\n" + discoveredMetadata)
        discoveredMetadata
      }
    }
  }

  def load(splitsDirO: SplitsDir)(implicit fs: FileSystem): Metadata = {
    val splitsMappingFile = new Path(splitsDirO.getMetadataPath + "/" + SPLITS_MAPPING_FILENAME)

    logger.debug("Loading metadata from: " + splitsMappingFile)

    var fsDataInputStream: FSDataInputStream = null
    try {
      fsDataInputStream = fs.open(splitsMappingFile)
      val metadataContent = scala.io.Source.fromInputStream(fsDataInputStream).getLines().mkString(LINE_SEPARATOR)
      deserialize(metadataContent.getBytes())
    } finally {
      if (fsDataInputStream != null) {
        fsDataInputStream.close()
      }
    }
  }

  def dump(md: Metadata, splitsDirO: SplitsDir)(implicit fs: FileSystem): Unit = {
    val splitsMappingFile = new Path(splitsDirO.getMetadataPath + "/" + SPLITS_MAPPING_FILENAME)

    logger.debug("Dumping metadata to: " + splitsMappingFile)

    var fsDataOutputStream: FSDataOutputStream = null
    try {
      fsDataOutputStream = fs.create(splitsMappingFile, true)
      fsDataOutputStream.write(serialize(md))
    } finally {
      if (fsDataOutputStream != null) {
        fsDataOutputStream.close()
      }
    }
  }

  private def serialize(md: Metadata): Array[Byte] = {
    val bigs = md.bigs.files.map(file => List(BIG_KEY, file.size, file.path.toString))
    val smalls = md.smalls.files.map(file => List(SMALL_KEY, file.size, file.path.toString))
    val splits = md.splits.files.map(file => List(SPLIT_KEY, file.size, file.path.toString))
    val all = bigs ++ smalls ++ splits
    all.map(_.mkString(ROW_SEPARATOR)).mkString(LINE_SEPARATOR).getBytes
  }

  private def deserialize(md: Array[Byte]): Metadata = {
    val s = new String(md)
    val lines = s.split(LINE_SEPARATOR).map(line => line.split(ROW_SEPARATOR))
    val bigs = lines.filter(line => line(0) == BIG_KEY).map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSet
    val smalls = lines.filter(line => line(0) == SMALL_KEY).map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSet
    val splits = lines.filter(line => line(0) == SPLIT_KEY).map(b => FileDetails(new Path(b(2)), b(1).toLong)).toSet
    Metadata(FileDetailsSet(splits), FileDetailsSet(bigs), FileDetailsSet(smalls))
  }

}
