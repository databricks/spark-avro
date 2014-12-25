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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

/**
 * Provides access to Avro data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource extends RelationProvider {

  /**
   * Creates a new relation for data store in avro given a `path` as a parameter.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    AvroRelation(parameters("path"))(sqlContext)
  }
}

