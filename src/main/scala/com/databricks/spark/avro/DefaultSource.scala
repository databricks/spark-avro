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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._

/**
 * Provides access to Avro data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for AVRO data."))
  }

  /**
   * Creates a new relation for data store in avro given a `path` as a parameter.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val minPartitions = parameters.getOrElse("minPartitions", "0").toInt
    AvroRelation(checkPath(parameters), None, minPartitions)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val minPartitions = parameters.getOrElse("minPartitions", "0").toInt
    AvroRelation(checkPath(parameters), Some(schema), minPartitions)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = parameters("path")
    val recordName = parameters.getOrElse(
      "recordName",
      AvroSaver.defaultParameters.get("recordName").get)
    val recordNamespace = parameters.getOrElse(
      "recordNamespace",
      AvroSaver.defaultParameters.get("recordNamespace").get)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore =>
          false
      }
    } else {
      true
    }

    if (doSave) {
      // Only save data when the save mode is not ignore.
      data.saveAsAvroFile(
        path,
        Map("recordName" -> recordName, "recordNamespace" -> recordNamespace))
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
