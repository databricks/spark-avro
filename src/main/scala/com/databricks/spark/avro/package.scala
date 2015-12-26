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

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrameReader, DataFrameWriter, DataFrame}

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
    * Adds a method, `schema`, to DataFrameReader that allows you to specific avro schema
    */
  implicit class AvroDataFrameReaderSchema(reader: DataFrameReader) {
    def schema(avroSchema: Schema): DataFrameReader = {
      val sqlSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      reader.schema(sqlSchema)
    }
  }
}
