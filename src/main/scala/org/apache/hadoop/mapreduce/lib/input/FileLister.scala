package org.apache.hadoop.mapreduce.lib.input

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce
import org.apache.spark.SparkContext

import scala.collection.mutable

object FileLister {

  def parse(path: String)(implicit sc: SparkContext): List[FileStatus] = {
    val files = mutable.ArrayBuffer[FileStatus]()
    val job = new mapreduce.Job(sc.hadoopConfiguration)
    FileInputFormat.addInputPaths(job, path)
    val it = new TextInputFormat().listStatus(job).iterator()
    while (it.hasNext) {
      files += it.next()
    }
    files.toList
  }

  def parseOnlyFiles(path: String)(implicit sc: SparkContext): List[FileStatus] = {
    parse(path).filter(fs => fs.isFile).distinct
  }

}

