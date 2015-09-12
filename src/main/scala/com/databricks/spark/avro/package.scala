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
package com.databricks.spark

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.JavaConversions._

import scala.collection.Iterator

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  @deprecated("use read.avro()", "1.1.0")
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(
        new AvroRelation(Array(filePath), None, None, Map.empty)(sqlContext))
  }

  /**
   * Adds a method, `avro`, to DataFrameWriter that allows you to write avro files using
   * the DataFileWriter
   */
  implicit class AvroDataFrameWriter(writer: DataFrameWriter) {
    def avro: String => Unit = writer.format("com.databricks.spark.avro").save
  }

  /**
   * Adds a method, `avro`, to DataFrameReader that allows you to read avro files using
   * the DataFileReade
   */
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def avro: String => DataFrame = reader.format("com.databricks.spark.avro").load
  }

  /**
   * Adds a method, `toRowRDD`, to RDDs of GenericRecords.  This allows you to convert it to
   * an RDD of Row (and, by implication, a DataFrame).
   */
  implicit class GenericRecordRDD(rdd: RDD[GenericRecord]) {
    def toRowRDD(requiredColumns: Array[String]): RDD[Row] = rdd.mapPartitions { records =>
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
