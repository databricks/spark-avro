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
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.reflect._

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  @deprecated("use read.avro()", "1.1.0")
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(
        new AvroRelation(Array(filePath), None, None, Map.empty)(sqlContext))

    def avroFileFromType[T <: SpecificRecord: ClassTag](filePath: String,
                                                        minPartitions: Int = 0): DataFrame = {
      import scala.reflect._
      val clazz = classTag[T].runtimeClass
      val avroSchema = SpecificData.get().getSchema(clazz)
      val sqlSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      sqlContext.baseRelationToDataFrame(
        new AvroRelation(Array(filePath), Some(sqlSchema), None, Map.empty)(sqlContext)
      )
    }
  }

  /**
   * Adds a method, `avro`, to DataFrameWriter that allows you to write avro files using
   * the DataFileWriter
   */
  implicit class AvroDataFrameWriter(writer: DataFrameWriter) {
    def avro: String => Unit = writer.format("com.databricks.spark.avro").save
  }

  /**
    * Adds a method `toAvroDF` to a compatible RDD
    */
  implicit class AvroDataFrameCreator[A <: GenericRecord with SpecificRecord: ClassTag](rdd: RDD[A])
  {
    def toAvroDF(sqLContext: SQLContext) : DataFrame = {
      val clazz = classTag[A].runtimeClass
      val avroSchema = SpecificData.get().getSchema(clazz)
      val sqlSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      val transformed = rdd.map { row =>
        val values = for (field <- sqlSchema.fields) yield {
          row.get(field.name)
        }
        Row(values:_*)
      }
      sqLContext.createDataFrame(transformed, sqlSchema)
    }
  }

  /**
   * Adds a method, `avro`, to DataFrameReader that allows you to read avro files using
   * the DataFileReade
   */
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def avro: String => DataFrame = reader.format("com.databricks.spark.avro").load
  }
}
