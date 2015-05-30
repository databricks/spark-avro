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

import org.apache.spark.sql.{SQLContext, DataFrame}

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(AvroRelation(filePath, None, minPartitions)(sqlContext))

  }

  /**
   * Adds a method, `saveAsAvroFile`, to DataFrame that allows you to save it as avro file.
   */
  implicit class AvroDataFrame(dataFrame: DataFrame) {
    def saveAsAvroFile(
        path: String,
        parameters: Map[String, String] = AvroSaver.defaultParameters): Unit =
      AvroSaver.save(dataFrame, path, parameters)

    def addAvroAliasColumns() : DataFrame =
      SchemaConverters.dataFrameWithAliasColumn(dataFrame)
  }
}
