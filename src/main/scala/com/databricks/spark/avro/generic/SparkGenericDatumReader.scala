package com.databricks.spark.avro.generic

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

 /**
  *  A generic datumreader that reads strings as string instead of utf-8
  */
class SparkGenericDatumReader extends GenericDatumReader[GenericRecord]{

  override def findStringClass(

    schema: Schema): Class[_] = classOf[String]

}
