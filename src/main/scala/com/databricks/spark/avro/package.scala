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
import org.apache.avro.generic.GenericRecord

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

package object avro {

  implicit class AvroContext(sqlContext: SQLContext) {
    
    @deprecated("use read.avro()", "1.1.0")
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(
        new AvroRelation(Array(filePath), None, None, Map.empty)(sqlContext))

    def createAvroDataFrame(rdd: RDD[GenericRecord], schema: Schema): DataFrame = 
      AvroUtil.createDataFrame(sqlContext, rdd, schema)
      
    def createAvroRdd(df: DataFrame, schema: Schema) : RDD[GenericRecord] = 
      AvroUtil.createRdd(df, schema)     

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
    
    // Adds a method, `schema`, to DataFrameReader that allows you to specify avro schema, e.g.
    //   val avroSchema = new Schema.Parser().parse(new File("avsc/user.avsc"))
    //   val df = sqlContext.read.schema(avroSchema).avro("avro/") 
    def schema(avroSchema: Schema): DataFrameReader = {
     val sqlSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
     reader.schema(sqlSchema)
   }
    
  }
  
  /*
  implicit class AvroSQLContext(sqlContext: SQLContext) {
    
    def createAvroDataFrame(rdd: RDD[GenericRecord], schema: Schema): DataFrame = 
      AvroUtil.createDataFrame(sqlContext, rdd, schema)
      
    def createAvroRdd(df: DataFrame, schema: Schema) : RDD[GenericRecord] = 
      AvroUtil.createRdd(df, schema) 
    
  }*/

}
