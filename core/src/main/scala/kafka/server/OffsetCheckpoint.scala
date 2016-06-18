/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import org.apache.kafka.common.utils.Utils

import scala.collection._
import kafka.utils.Logging
import kafka.common._
import java.io._
import java.nio.channels.FileChannel
import java.nio.file.Files

import scala.util.Try

object OffsetCheckpoint {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

/**
 * This class saves out a map of topic/partition=>offsets to a file
 */
class OffsetCheckpoint(val file: File) extends Logging {
  import OffsetCheckpoint._
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  private var version = 0L
  private val maxVersions = 10

  private val filenameFilter = new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = {
      name.startsWith(file.getName)
    }
  }

  new File(file + ".tmp").delete() // try to delete any existing temp files for cleanliness

  private implicit val ordering = new Ordering[File] {
    def compare(a: File, b: File): Int = {
      val v1 = fileVersion(a)
      val v2 = fileVersion(b)
      if (v1 < v2) {
        -1
      } else if (v1 == v2) {
        0
      } else {
        1
      }
    }
  }

  private def children = {
    val list = file.getParentFile.listFiles(filenameFilter)
    scala.util.Sorting.quickSort(list)
    list
  }

  private def currentFile = {
    val file = children.lastOption.getOrElse(new File(fileName(version)))
    file.createNewFile()
    file
  }

  private def fileName(number: Long): String = {
    "%s.%x".format(file.getAbsolutePath, number)
  }

  private def fileVersion(f: File): Long = {
    try {
      java.lang.Long.valueOf(f.getName.split('.').last, 16).longValue()
    } catch {
      case e: Throwable => 0L
    }
  }

  version = children
    .lastOption
    .map(fileVersion)
    .getOrElse(0L)

  for (f <- children.dropRight(maxVersions)) {
    f.delete()
  }

  private def syncDir(): Unit = {
    // fsync dir
    var dir: FileChannel = null
    try {
      dir = FileChannel.open(file.getParentFile.toPath, java.nio.file.StandardOpenOption.READ)
      dir.force(true)
    } catch {
      case e: Throwable =>
    } finally {
      if (dir != null) {
        dir.close()
      }
    }
  }

  def write(offsets: Map[TopicAndPartition, Long]) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      version += 1
      val currentFile = new File(fileName(version))

      val fileOutputStream = new FileOutputStream(currentFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        writer.write(CurrentVersion.toString)
        writer.newLine()

        writer.write(offsets.size.toString)
        writer.newLine()

        offsets.foreach { case (topicPart, offset) =>
          writer.write(s"${topicPart.topic} ${topicPart.partition} $offset")
          writer.newLine()
        }

        writer.flush()
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Runtime.getRuntime.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }

      new File(fileName(version - maxVersions)).delete()

      file.delete()
      Files.createLink(file.toPath, currentFile.toPath)

      syncDir()
    }
  }

  def malformedLineException(line: String) =
    new IOException(s"Malformed line in offset checkpoint file: $line'")

  def read(): Map[TopicAndPartition, Long] = {
    var result = Map[TopicAndPartition, Long]()
    val files = children
    var idx = files.length - 1
    var searching = true
    while (searching && idx >= 0) {
      try {
        if (idx != files.length - 1) {
          warn("try to load previous snapshot: %s".format(files(idx)))
        }
        result = readOne(files(idx))
        searching = result.isEmpty
      } catch {
        case e: IOException =>
          warn("broken snapshot: %s".format(files(idx)), e)
      }

      idx -= 1
    }
    result
  }

  def readOne(file: File): Map[TopicAndPartition, Long] = {
    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        line = reader.readLine()
        if (line == null)
          return Map.empty
        val version = line.toInt
        version match {
          case CurrentVersion =>
            line = reader.readLine()
            if (line == null)
              return Map.empty
            val expectedSize = line.toInt
            val offsets = mutable.Map[TopicAndPartition, Long]()
            line = reader.readLine()
            while (line != null) {
              WhiteSpacesPattern.split(line) match {
                case Array(topic, partition, offset) =>
                  offsets += TopicAndPartition(topic, partition.toInt) -> offset.toLong
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            if (offsets.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
            offsets
          case _ =>
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } catch {
        case e: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
  
}
