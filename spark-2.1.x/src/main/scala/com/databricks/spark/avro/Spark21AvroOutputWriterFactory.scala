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
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

private[avro] class Spark21AvroOutputWriterFactory(
    schema: StructType,
    recordName: String,
    recordNamespace: String) extends OutputWriterFactory {

  def doGetDefaultWorkFile(path: String, context: TaskAttemptContext, extension: String): Path = {
    new Path(path)
  }

  def newInstance(
       path: String,
       dataSchema: StructType,
       context: TaskAttemptContext): OutputWriter = {

    val ot = Class.forName("com.databricks.spark.avro.AvroOutputWriter")
    val meth = ot.getDeclaredConstructor(
      classOf[String], classOf[TaskAttemptContext], classOf[StructType],
      classOf[String], classOf[String],
      classOf[Function3[String, TaskAttemptContext, String, Path]]
    )
    meth.setAccessible(true)
    meth.newInstance(path, context, schema, recordName, recordNamespace, doGetDefaultWorkFile _)
      .asInstanceOf[OutputWriter]
  }

  override def getFileExtension(context: TaskAttemptContext): String = ".avro"
}
