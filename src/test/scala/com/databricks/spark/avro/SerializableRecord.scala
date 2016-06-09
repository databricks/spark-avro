package com.databricks.spark.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class SerializableRecord extends GenericRecord with Serializable {

  private var content: String = null

  override def get(key: String): AnyRef = {
    assert(key == "content")
    content
  }

  override def put(key: String, v: scala.Any): Unit = {
    assert(key == "content")
    content = v.toString
  }

  override def get(i: Int): AnyRef = {
    assert(i == 0)
    content
  }

  override def put(i: Int, v: scala.Any): Unit = {
    assert(i == 0)
    content = v.toString
  }

  override def getSchema: Schema = {
    val parser = new Schema.Parser()
    parser.parse(
      """{
        |  "type": "record",
        |  "name": "test_record",
        |  "fields": [{
        |    "name": "content",
        |    "type": "string"
        |  }]
        |}
      """.stripMargin)
  }
}