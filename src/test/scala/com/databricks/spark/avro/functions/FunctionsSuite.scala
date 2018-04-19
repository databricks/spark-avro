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
package com.databricks.spark.avro.functions

import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import scala.util.Random

class FunctionsSuite extends FunSuite with BeforeAndAfterAll
  with GeneratorDrivenPropertyChecks with Matchers {

  import Arbitrary.arbitrary

  private var spark: SparkSession = _

  private val recordSchema = SchemaBuilder.record("test").fields()
    .requiredLong("id")
    .optionalBoolean("disabled")
    .optionalString("name")
    .optionalInt("count")
    .optionalDouble("amount")
    .endRecord()

  private val recGen: Gen[GenericData.Record] = for {
    id <- Gen.posNum[Long]
    disabled <- arbitrary[Boolean]
    name <- Gen.chooseNum(0, 100).map(Random.nextString)
    count <- arbitrary[Int]
    amount <- arbitrary[Double]
  } yield {
    val rec = new GenericData.Record(recordSchema)
    rec.put("id", id)
    rec.put("disabled", disabled)
    rec.put("name", name)
    rec.put("count", count)
    rec.put("amount", amount)
    rec
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("FunctionsSuite")
      .config("spark.sql.files.maxPartitionBytes", 1024)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  case class Record(idx: Int, record: Array[Byte])

  test("reading serialized data") {
    forAll(Gen.nonEmptyListOf(recGen)) { records =>
      val rows = records.zipWithIndex.map { case (rec, idx) =>
        Record(idx, serialize(recordSchema, rec))
      }
      val avroSerDf = spark.createDataFrame(rows)

      val decodedDf = avroSerDf
        .withColumn("avrorec", from_avro(col("record"), recordSchema.toString(false)))
        .select(col("idx"), col("avrorec.*"))
        .orderBy(col("idx"))

      decodedDf.columns should contain allOf("idx", "id", "disabled", "name", "count", "amount")
      val decodedRows = decodedDf.collect()
      decodedRows should have length records.length
      decodedRows.map(rowToGenericRecord).toList shouldEqual(records)
    }
  }

  private def serialize(schema: Schema, record: GenericData.Record): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val outputDatumWriter = new GenericDatumWriter[GenericData.Record](schema)
    val encoder = EncoderFactory.get.binaryEncoder(baos, null)
    outputDatumWriter.write(record, encoder)
    encoder.flush()
    baos.toByteArray
  }

  def rowToGenericRecord(row: Row): GenericData.Record = {
    val rec = new GenericData.Record(recordSchema)
    rec.put("id", row.getAs[Long]("id"))
    rec.put("disabled", row.getAs[Boolean]("disabled"))
    rec.put("name", row.getAs[String]("name"))
    rec.put("count", row.getAs[Int]("count"))
    rec.put("amount", row.getAs[Double]("amount"))
    rec
  }
}

