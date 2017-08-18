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

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.sql.Date
import java.util
import java.util.HashMap

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import scala.collection.immutable.Map
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, TaskAttemptID}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[avro] class AvroOutputWriter(
    path: String,
    context: TaskAttemptContext,
    schema: StructType,
    recordName: String,
    recordNamespace: String,
    forceSchema: String) extends OutputWriter  {

  private val logger = Logger.getLogger(this.getClass)

  private val forceAvroSchema = if (forceSchema.contentEquals("")) {
    None
  } else {
    Option(new Schema.Parser().parse(forceSchema))
  }
  private lazy val converter = createConverterToAvro(
    schema, recordName, recordNamespace, forceAvroSchema
  )

  // copy of the old conversion logic after api change in SPARK-19085
  private lazy val internalRowConverter =
    CatalystTypeConverters.createToScalaConverter(schema).asInstanceOf[InternalRow => Row]

  /**
   * Overrides the couple of methods responsible for generating the output streams / files so
   * that the data can be correctly partitioned
   */
  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
    new AvroKeyOutputFormat[GenericRecord]() {

      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }

      @throws(classOf[IOException])
      override def getAvroFileOutputStream(c: TaskAttemptContext): OutputStream = {
        val path = getDefaultWorkFile(context, ".avro")
        path.getFileSystem(context.getConfiguration).create(path)
      }

    }.getRecordWriter(context)

  // this is the new api in spark 2.2+
  def write(row: InternalRow): Unit = {
    write(internalRowConverter(row))
  }

  // api in spark 2.0 - 2.1
  def write(row: Row): Unit = {
    val key = new AvroKey(converter(row).asInstanceOf[GenericRecord])
    recordWriter.write(key, NullWritable.get())
  }

  override def close(): Unit = recordWriter.close(context)

  private def resolveStructTypeToAvroUnion(schema:Schema, dataType:String): Schema = {
    val allowedAvroTypes = dataType match {
      case "boolean"  => List(Schema.Type.BOOLEAN)
      case "integer"  => List(Schema.Type.INT)
      case "long"     => List(Schema.Type.LONG)
      case "float"    => List(Schema.Type.FLOAT)
      case "double"   => List(Schema.Type.DOUBLE)
      case "binary"   => List(Schema.Type.BYTES, Schema.Type.FIXED)
      case "array"    => List(Schema.Type.ARRAY)
      case "map"      => List(Schema.Type.MAP)
      case "string"   => List(Schema.Type.STRING, Schema.Type.ENUM)
      case "struct"   => List(Schema.Type.ARRAY, Schema.Type.RECORD)
      case default    => {
        throw new RuntimeException(
          s"Cannot map SparkSQL type '$dataType' against Avro schema '$schema'"
        )
      }
    }
    schema.getTypes.find (allowedAvroTypes contains _.getType).get
  }

  /**
   * This function constructs converter function for a given sparkSQL datatype. This is used in
   * writing Avro records out to disk
   */
  private def createConverterToAvro(
      dataType: DataType,
      structName: String,
      recordNamespace: String,
      forceAvroSchema: Option[Schema]): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => if (forceAvroSchema.isDefined) {
          // Handle mapping from binary => bytes|fixed w/ forceSchema
          forceAvroSchema.get.getType match {
            case Schema.Type.BYTES => ByteBuffer.wrap(bytes)
            case Schema.Type.FIXED => new GenericData.Fixed(
              forceAvroSchema.get, bytes
            )
            case default => bytes
          }
        } else {
          ByteBuffer.wrap(bytes)
        }
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | BooleanType => identity
      case StringType => (item: Any) => if (forceAvroSchema.isDefined) {
        // Handle case when forcing schema where this string should map
        // to an ENUM
        forceAvroSchema.get.getType match {
          case Schema.Type.ENUM => new GenericData.EnumSymbol(
            forceAvroSchema.get, item.toString
          )
          case default => item
        }
      } else {
        item
      }
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = if (forceAvroSchema.isDefined) {
          createConverterToAvro(elementType, structName,
            recordNamespace, Option(forceAvroSchema.get.getElementType))
        } else {
          createConverterToAvro(elementType, structName,
            recordNamespace, forceAvroSchema)
        }
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetArray = new Array[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray(idx) = elementConverter(sourceArray(idx))
              idx += 1
            }
            targetArray.toSeq.asJava
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = if (forceAvroSchema.isDefined) {
          createConverterToAvro(valueType, structName,
            recordNamespace, Option(forceAvroSchema.get.getValueType))
        } else {
          createConverterToAvro(valueType, structName,
            recordNamespace, forceAvroSchema)
        }
        (item: Any) => {
          if (item == null) {
            if (forceAvroSchema.isDefined) new HashMap[String, Any]() else null
          } else {
            val javaMap = new HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach { case (key, value) =>
              javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = if (!forceAvroSchema.isDefined) {
          SchemaConverters.convertStructToAvro(
            structType, builder, recordNamespace)
        } else {
          if (forceAvroSchema.get.getType == Schema.Type.ARRAY) {
            forceAvroSchema.get.getElementType
          } else {
            forceAvroSchema.get
          }
        }

        val fieldConverters = structType.fields.map (
          field => {
            val fieldConvertSchema = if (forceAvroSchema.isDefined) {
              val thisFieldSchema = schema.getField(field.name).schema
              Option(
                thisFieldSchema.getType match {
                  case Schema.Type.UNION => {
                    resolveStructTypeToAvroUnion(thisFieldSchema, field.dataType.typeName)
                  }
                  case default => thisFieldSchema
                }
              )
            } else {
              forceAvroSchema
            }
            createConverterToAvro(field.dataType, field.name, recordNamespace, fieldConvertSchema)
          }
        )

        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              val fieldValue = rowIterator.next()
              val fieldName = fieldNamesIterator.next()
              try {
                record.put(fieldName, converter(fieldValue))
              } catch {
                case ex:NullPointerException => {
                  // This can happen with forceAvroSchema conversion
                  if (forceAvroSchema.isDefined) {
                    logger.debug(s"Trying to write field $fieldName which may be null? $fieldValue")
                  } else {
                    // Keep previous behavior when forceAvroSchema is not used
                    throw ex
                  }
                }
              }
            }
            if(forceAvroSchema.isDefined) {
              new GenericRecordBuilder(record).build()
            } else {
              record
            }
          }
        }
    }
  }
}
