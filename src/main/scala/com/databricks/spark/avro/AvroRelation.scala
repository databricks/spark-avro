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

import java.nio.ByteBuffer
import java.util.Map

import scala.collection.JavaConversions._

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.{Fixed, EnumSymbol, Record}
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.avro.mapred.FsInput
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.sources.TableScan

case class AvroRelation(location: String)(@transient val sqlContext: SQLContext) extends TableScan {

  override val schema = {
    val fileReader = newReader()
    val convertedSchema = toSqlType(fileReader.getSchema).dataType match {
      case s: StructType => s
      case other =>
        sys.error(s"Avro files must contain Records to be read, type $other not supported")
    }
    fileReader.close()
    convertedSchema
  }

  /** 
   * There are several properties of Avro that we have to account for in this method:
   * 1) Avro uses Utf8 strings, so we want to convert them to java.lang.String for SparkSQL,
   *    including making the appropriate recursive calls.
   * 2) Avro map keys are always strings, so we don't need to recurse on them when processing maps.
   * 3) Byte arrays and ByteBuffers are reused by Avro, so we must copy them out. 
   */
  private def convertToSparkSQL(obj: Any): Any = {
    obj match {
      case u: Utf8 =>
        u.toString
      case m: Map[Any, Any] =>
        m.map(x => (x._1.toString, convertToSparkSQL(x._2)))
      case avroArray: GenericData.Array[Any] =>
        val javaArray = new Array[Any](avroArray.size)
        var idx = 0
        while (idx < avroArray.size) {
          javaArray(idx) = convertToSparkSQL(avroArray(idx))
          idx += 1
        }
        javaArray.toSeq
      case f: Fixed =>
        f.bytes.clone
      case e: EnumSymbol =>
        e.toString
      case r: Record => 
        Row.fromSeq((0 until r.getSchema.getFields.size).map { i =>
          convertToSparkSQL(r.get(i))
        })
      case avroBytes: ByteBuffer =>
        val javaBytes = new Array[Byte](avroBytes.remaining)
        avroBytes.get(javaBytes)
        javaBytes
      case other =>
        other
    }
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  override lazy val buildScan = {
    val baseRdd = sqlContext.sparkContext.hadoopFile(
      location,
      classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
      classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
      classOf[org.apache.hadoop.io.NullWritable],
      sqlContext.sparkContext.defaultMinPartitions)

    baseRdd.map { record =>
      val values = (0 until schema.fields.size).map { i =>
        convertToSparkSQL(record._1.datum().get(i))
      }

      Row.fromSeq(values)
    }
  }

  private def newReader() = {
    val path = new Path(location)
    val fs = FileSystem.get(path.toUri, sqlContext.sparkContext.hadoopConfiguration)

    val status = fs.getFileStatus(path)
    val singleFile = if (status.isDir) {
      fs.listStatus(path)
        .find(_.getPath.toString endsWith "avro")
        .map(_.getPath)
        .getOrElse(sys.error(s"Could not find .avro file with schema at $path"))
    } else {
      path
    }
    val input = new FsInput(singleFile, sqlContext.sparkContext.hadoopConfiguration)
    val reader = new GenericDatumReader[GenericRecord]()
    DataFileReader.openReader(input, reader)
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)

  private def toSqlType(avroSchema: Schema): SchemaType = {
    import Schema.Type._

    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.get(0)).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes)).copy(nullable = true)
          }
        } else avroSchema.getTypes.map(_.getType) match {
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other =>
            sys.error(s"This mix of union types is not supported (see README): $other")
        }

      case other => sys.error(s"Unsupported type $other")
    }
  }
}
