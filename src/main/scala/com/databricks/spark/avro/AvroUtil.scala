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

import scala.collection.Iterator
import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object AvroUtil {

  /**
   * Create a DataFrame from an RDD of GenericRecords, with columns based on the provided schema
   */
  def createDataFrame(
      sqlContext: SQLContext,
      rdd: RDD[GenericRecord],
      schema: Schema): DataFrame = {
    sqlContext.createDataFrame(
      convertRecordRDDToRowRDD(rdd, schema.getFields.map(_.name()).toArray),
      SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType])
  }

  /**
   * Java API to create a DataFrame from an RDD of GenericRecords, with columns based on the
   * provided schema.
   */
  def createDataFrame(
      sqlContext: SQLContext,
      rdd: JavaRDD[GenericRecord],
      schema: Schema): DataFrame = {
    createDataFrame(sqlContext, rdd.rdd, schema)
  }

  private[avro] def convertRecordRDDToRowRDD(
      rdd: RDD[GenericRecord],
      requiredColumns: Array[String]): RDD[Row] = {
    rdd.mapPartitions { records =>
      if (records.isEmpty) {
        Iterator.empty
      } else {
        val firstRecord = records.next()
        val superSchema = firstRecord.getSchema // the schema of the actual record
        // the fields that are actually required along with their converters
        val avroFieldMap = superSchema.getFields.map(f => (f.name, f)).toMap

        new Iterator[Row] {
          private[this] val baseIterator = records
          private[this] var currentRecord = firstRecord
          private[this] val rowBuffer = new Array[Any](requiredColumns.length)
          // A micro optimization to avoid allocating a WrappedArray per row.
          private[this] val bufferSeq = rowBuffer.toSeq

          // An array of functions that pull a column out of an avro record and puts the
          // converted value into the correct slot of the rowBuffer.
          private[this] val fieldExtractors = requiredColumns.zipWithIndex.map {
            case (columnName, idx) =>
              // Spark SQL should not pass us invalid columns
              val field =
                avroFieldMap.getOrElse(
                  columnName,
                  throw new AssertionError(s"Invalid column $columnName"))
              val converter = SchemaConverters.createConverterToSQL(field.schema)

              (record: GenericRecord) => rowBuffer(idx) = converter(record.get(field.pos()))
          }

          private def advanceNextRecord() = {
            if (baseIterator.hasNext) {
              currentRecord = baseIterator.next()
              true
            } else {
              false
            }
          }

          def hasNext = {
            currentRecord != null || advanceNextRecord()
          }

          def next() = {
            assert(hasNext)
            var i = 0
            while (i < fieldExtractors.length) {
              fieldExtractors(i)(currentRecord)
              i += 1
            }
            currentRecord = null
            Row.fromSeq(bufferSeq)
          }
        }
      }
    }
  }

}
