package com.databricks.spark.avro.generic

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

class SparkGenericDatumReader extends GenericDatumReader[GenericRecord]{

  override def findStringClass(
    schema: Schema): Class[_] = classOf[String] // avoid utf-8 strings

}
