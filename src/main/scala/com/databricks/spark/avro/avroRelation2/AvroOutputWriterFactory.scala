package com.databricks.spark.avro.avroRelation2

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.sources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType


private[avro] class AvroOutputWriterFactory(schema: StructType,
                                            recordName: String,
                                            recordNamespace: String) extends OutputWriterFactory {

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter =
    new AvroOutputWriter(path, context, schema, recordName, recordNamespace)
}
