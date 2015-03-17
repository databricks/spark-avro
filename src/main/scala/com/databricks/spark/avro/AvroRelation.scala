/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.avro

import java.io.{IOException, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.Map

import scala.collection.JavaConversions._

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.{Fixed, Record}
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.avro.mapred.FsInput
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.Schema.Type._

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}


case class AvroRelation(
    location: String,
    userSpecifiedSchema: Option[StructType],
    minPartitions: Int = 0) (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with InsertableRelation {
  var avroSchema: Schema = null

  override val schema = {
    if (userSpecifiedSchema.isDefined) {
      // We need avroSchema to construct converter
      avroSchema = SchemaConverters.convertStructToAvro(userSpecifiedSchema.get,
        SchemaBuilder.record("topLevelRecord"))
      userSpecifiedSchema.get
    } else {
      val fileReader = newReader()
      try {
        avroSchema = fileReader.getSchema
      } finally {
        fileReader.close()
      }
      val convertedSchema = SchemaConverters.toSqlType(avroSchema).dataType match {
        case s: StructType => s
        case other =>
          sys.error(s"Avro files must contain Records to be read, type $other not supported")
      }
      convertedSchema
    }
  }

  override def buildScan = {
    val minPartitionsNum = if (minPartitions <= 0) {
      sqlContext.sparkContext.defaultMinPartitions
    } else {
      minPartitions
    }

    val baseRdd = sqlContext.sparkContext.hadoopFile(
      location,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable],
      minPartitionsNum)

    val converter = createConverter(avroSchema)
    baseRdd.map(record => converter(record._1.datum).asInstanceOf[Row])
  }

  private def getAllFiles(fs: FileSystem)(path: Path): Stream[Path] = {
    if (fs.isDirectory(path)) {
      fs.listStatus(path).toStream.map(_.getPath).flatMap(getAllFiles(fs)(_))
    } else {
      Stream(path)
    }
  }

  private def newReader() = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)
    val globStatus = fs.globStatus(path)

    if (globStatus == null) {
      throw new FileNotFoundException(s"The path you've provided ($location) is invalid.")
    }

    val statuses = globStatus
      .toStream
      .map(_.getPath)
      .flatMap(getAllFiles(fs)(_))
    val singleFile = statuses
      .find(_.getName.endsWith("avro"))
      .getOrElse(sys.error(s"Could not find .avro file with schema at $path"))

    val input = new FsInput(singleFile, sqlContext.sparkContext.hadoopConfiguration)
    val reader = new GenericDatumReader[GenericRecord]()
    DataFileReader.openReader(input, reader)
  }

  /**
   * This function constructs a converter function that will be used to convert avro types to their
   * corresponding sparkSQL representations.
   */
  private def createConverter(schema: Schema): (Any) => Any = {
    schema.getType match {
      case STRING | ENUM =>
        // Avro strings are in Utf8, so we have to call toString on them
        (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG =>
        (item: Any) => item

      case BYTES =>
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val avroBytes = item.asInstanceOf[ByteBuffer]
            val javaBytes = new Array[Byte](avroBytes.remaining)
            avroBytes.get(javaBytes)
            javaBytes
          }
        }

      case FIXED =>
        // Byte arrays are reused by avro, so we have to make a copy of them.
        (item: Any) => if (item == null) null else item.asInstanceOf[Fixed].bytes.clone

      case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverter(f.schema))

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = item.asInstanceOf[GenericRecord]
            val converted = new Array[Any](fieldConverters.size)
            var idx = 0
            while (idx < fieldConverters.size) {
              converted(idx) = fieldConverters.apply(idx)(record.get(idx))
              idx += 1
            }

            Row.fromSeq(converted.toSeq)
          }
        }

      case ARRAY =>
        val elementConverter = createConverter(schema.getElementType)

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val avroArray = item.asInstanceOf[GenericData.Array[Any]]
            val convertedArray = new Array[Any](avroArray.size)
            var idx = 0
            while (idx < avroArray.size) {
              convertedArray(idx) = elementConverter(avroArray(idx))
              idx += 1
            }
            convertedArray.toSeq
          }
        }

      case MAP =>
        val valueConverter = createConverter(schema.getValueType)

        (item: Any) => {
          if (item == null) {
            null
          } else {
            // Avro map keys are always strings, so it's enough to just call toString on them.
            item.asInstanceOf[Map[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
          }
        }

      case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverter(remainingUnionTypes.get(0))
          } else {
            createConverter(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
          case other =>
            sys.error(s"This mix of union types is not supported (see README): $other")
        }

      case other => sys.error(s"Unsupported type $other")
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val filesystemPath = new Path(location)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a AVRO table:", e)
      }
      // Write the data.
      data.saveAsAvroFile(location)
      // Right now, we assume that the schema is not changed. We will not update the schema.
      // schema = data.schema
    } else {
      sys.error("AVRO tables only support INSERT OVERWRITE for now.")
    }
  }
}
