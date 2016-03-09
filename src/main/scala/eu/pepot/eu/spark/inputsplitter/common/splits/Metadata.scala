package eu.pepot.eu.spark.inputsplitter.common.splits

import eu.pepot.eu.spark.inputsplitter.common.file.{FileDetails, FileDetailsSet, Mappings}
import eu.pepot.eu.spark.inputsplitter.common.splits.resolvers.MetadataResolver
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.slf4j.LoggerFactory

case class Metadata(
  mappings: Mappings,
  bigs: FileDetailsSet,
  smalls: FileDetailsSet
) {

  def splits() = {
    mappings.bigsToSplits.map(_._2)
  }

  def smallsAndSplits() = {
    smalls.files ++ mappings.bigsToSplits.map(_._2)
  }

  def resolve(
    discSplits: FileDetailsSet,
    discBigs: FileDetailsSet,
    discSmalls: FileDetailsSet
  )(implicit metadataResolver: MetadataResolver) = {
    metadataResolver.resolve(this, discSplits, discBigs, discSmalls)
  }


}

object Metadata {

  val logger = LoggerFactory.getLogger(this.getClass)

  val BIG_KEY = "I"
  val SMALL_KEY = "i"
  val MAPPING_KEY = "s"
  val ROW_SEPARATOR = ","
  val LINE_SEPARATOR = "\n"

  val SPLITS_MAPPING_FILENAME = "splits.mapping"

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
    val bigs = md.bigs.files.map(file => List(BIG_KEY, file.path.toString, file.size))
    val smalls = md.smalls.files.map(file => List(SMALL_KEY, file.path.toString, file.size))
    val mappings = md.mappings.bigsToSplits.map{case (b, s) => List(MAPPING_KEY, b.path, b.size, s.path, s.size)}
    val all = bigs ++ smalls ++ mappings
    all.map(_.mkString(ROW_SEPARATOR)).mkString(LINE_SEPARATOR).getBytes
  }

  private def deserialize(md: Array[Byte]): Metadata = {
    val s = new String(md)
    val lines = s.split(LINE_SEPARATOR).map(line => line.split(ROW_SEPARATOR))
    val bigs = lines.filter(line => line(0) == BIG_KEY).map(b => FileDetails(b(1), b(2).toLong)).toSet
    val smalls = lines.filter(line => line(0) == SMALL_KEY).map(b => FileDetails(b(1), b(2).toLong)).toSet
    val mappings = lines.filter(line => line(0) == MAPPING_KEY).map(b => (FileDetails(b(1), b(2).toLong), FileDetails(b(3), b(4).toLong))).toSet
    Metadata(Mappings(mappings), FileDetailsSet(bigs), FileDetailsSet(smalls))
  }

}
