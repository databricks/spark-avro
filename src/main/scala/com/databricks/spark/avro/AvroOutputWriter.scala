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
import java.sql.Timestamp
import java.util.HashMap

import scala.collection.immutable.Map

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.OutputWriter
import org.apache.spark.sql.types._

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[avro] class AvroOutputWriter(path: String,
    context: TaskAttemptContext,
    schema: StructType,
    recordName: String,
    recordNamespace: String) extends OutputWriter  {

  private lazy val converter = createConverterToAvro(schema, recordName, recordNamespace)

  private val recordWriter: RecordWriter[AvroKey[GenericRecord], NullWritable] =
    new AvroKeyOutputFormat[GenericRecord]().getRecordWriter(context)

  override def write(row: Row): Unit = {
    val key = new AvroKey(converter(row).asInstanceOf[GenericRecord])
    recordWriter.write(key, NullWritable.get())
  }

  override def close(): Unit = recordWriter.close(context)

  /**
   * This function constructs converter function for a given sparkSQL datatype. This is used in
   * writing Avro records out to disk
   */
  private def createConverterToAvro(
      dataType: DataType,
      structName: String,
      recordNamespace: String): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(elementType, structName, recordNamespace)
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
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(valueType, structName, recordNamespace)
        (item: Any) => {
          if (item == null) {
            null
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
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(field.dataType, field.name, recordNamespace))
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
              val fieldName = fieldNamesIterator.next()
              if (schema.getField(fieldName) != null) {
                record.put(fieldName, converter(rowIterator.next()))
              } else {
                rowIterator.next()
              }
            }
            record
          }
        }
    }
  }
}
