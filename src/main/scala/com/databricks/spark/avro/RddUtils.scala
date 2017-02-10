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

import SchemaConverters._
import scala.util.Try
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD

/**
  * [[RDD]] implicits.
  */
object RddUtils {
  /**
    * Extensions to [[RDD]]s of [[GenericRecord]]s.
    *
    * @param rdd the [[RDD]] to decorate with additional functionality.
    */
  implicit class RddToDataFrame(val rdd: RDD[GenericRecord]) {
    /**
      * Convert a [[RDD]] of [[GenericRecord]]s to a [[DataFrame]]
      *
      * @return the [[DataFrame]]
      */
    def toDF(): DataFrame = {
      val spark = SparkSession
        .builder
        .config(rdd.sparkContext.getConf)
        .getOrCreate()

      val avroSchema = rdd.take(1)(0).getSchema
      val dataFrameSchema = toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      val converter = createConverterToSQL(avroSchema, dataFrameSchema)

      val rowRdd = rdd.flatMap { record =>
        Try(converter(record).asInstanceOf[Row]).toOption
      }

      spark.createDataFrame(rowRdd, dataFrameSchema)
    }
  }
}
