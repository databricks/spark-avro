/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.TestSQLContext

import scala.collection.JavaConversions._

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String) =
      sqlContext.baseRelationToSchemaRDD(AvroRelation(filePath)(sqlContext))
  }

  implicit class AvroSchemaRDD(schemaRDD: SchemaRDD) {
    def saveAsAvroFile(path: String): Unit = {
      /*
      val avroSchema = toAvroSchema(schemaRDD.schema)
      val convertedRDD = schemaRDD.map {

      }

      convertedRDD.saveAsNewAPIHadoopFile(outputPath,
        classOf[AvroKey[GenericRecord]],
        classOf[org.apache.hadoop.io.NullWritable],
        classOf[AvroKeyOutputFormat[GenericRecord]],
        job.getConfiguration)
        */
    }
  }
}

