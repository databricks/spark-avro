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

import org.apache.spark.sql.{SQLContext, SchemaRDD}

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String) =
      sqlContext.baseRelationToSchemaRDD(AvroRelation(filePath)(sqlContext))
  }

  // TODO: Implement me.
  implicit class AvroSchemaRDD(schemaRDD: SchemaRDD) {
    def saveAsAvroFile(path: String): Unit = ???
  }
}
