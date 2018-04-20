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
import java.nio.ByteBuffer

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FunSuite, Ignore, Matchers}

import scala.collection.JavaConverters._
import scala.util.Random

class FunctionsSuite extends FunSuite with BeforeAndAfterAll
  with GeneratorDrivenPropertyChecks with Matchers {

  import Arbitrary.arbitrary

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("FunctionsSuite")
      .config("spark.sql.files.maxPartitionBytes", 64)
      .getOrCreate()
  }

  implicit val arbString: Arbitrary[String] = Arbitrary(Gen.choose(0, 20).map(Random.nextString))

  private val thingSchema: Schema = SchemaBuilder.record("thing").fields()
    .optionalString("name")
    .optionalString("value")
    .endRecord
  private val arrayOfThingsSchema = SchemaBuilder.array().items(thingSchema)

  private val recordSchema = SchemaBuilder.record("test").fields()
    .requiredLong("id")
    .optionalBoolean("disabled")
    .optionalString("name")
    .optionalInt("count")
    .optionalDouble("amount")
    .name("things").`type`(arrayOfThingsSchema).noDefault()
    .endRecord()

  private val avroSchemaJson: String = recordSchema.toString(false)

  private val thingGen: Gen[GenericRecord] = for {
    name <- arbitrary[String]
    value <- arbitrary[String]
  } yield {
    val rec = new GenericData.Record(thingSchema)
    rec.put("name", name)
    rec.put("value", value)
    rec
  }

  private val recGen: Gen[GenericRecord] = for {
    id <- Gen.posNum[Long]
    disabled <- arbitrary[Boolean]
    name <- arbitrary[String]
    count <- arbitrary[Int]
    amount <- arbitrary[Double]
    things <- Gen.listOf(thingGen)
  } yield {
    val rec = new GenericData.Record(recordSchema)
    rec.put("id", id)
    rec.put("disabled", disabled)
    rec.put("name", name)
    rec.put("count", count)
    rec.put("amount", amount)
    rec.put("things", new GenericData.Array[GenericRecord](arrayOfThingsSchema, things.asJava))
    rec
  }

  private case class SerRecord(idx: Int, record: Array[Byte])


  test("serialize and deserialize test avro record") {
    val baos = new ByteArrayOutputStream()
    forAll(recGen) { rec =>
      baos.reset()
      val bytes = serialize(recordSchema, rec, baos)
      val recDes = deserialize(recordSchema, bytes, 0)
      rec shouldEqual recDes
    }
  }

  test("reading serialized data") {
    forAll(Gen.choose(0, 4), Gen.nonEmptyListOf(recGen)) { (headerSize, records) =>
      val baos = new ByteArrayOutputStream()

      val rows = records.zipWithIndex.map { case (rec, idx) =>
        baos.reset()
        writeHeader(headerSize, baos)
        SerRecord(idx, serialize(recordSchema, rec, baos))
      }
      val avroSerDf = spark.createDataFrame(rows)

      val decodedDf = avroSerDf
        .withColumn("avrorec", from_avro(col("record"), avroSchemaJson, headerSize))
        .select(col("idx"), col("avrorec.*"))
        .orderBy(col("idx"))

      decodedDf.columns should contain allOf("idx", "id", "disabled", "name", "count", "amount")
      val decodedRows = decodedDf.collect()
      decodedRows should have length records.length
      val decodedRecords = decodedRows.map(rowToGenericRecord).toList
      decodedRecords shouldEqual records
    }
  }

  /**
    * Writes given number of bytes with 0xFF simulating a record header of given size.
    *
    * @param headerSize Header size
    * @param baos Byte array output stream
    */
  private def writeHeader(headerSize: Int, baos: ByteArrayOutputStream): Unit = {
    for (_ <- 1 to headerSize) {
      baos.write(0xFF)
    }
  }

  private def rowToGenericRecord(row: Row): GenericData.Record = {
    val rec = new GenericData.Record(recordSchema)
    rec.put("id", row.getAs[Long]("id"))
    rec.put("disabled", row.getAs[Boolean]("disabled"))
    rec.put("name", row.getAs[String]("name"))
    rec.put("count", row.getAs[Int]("count"))
    rec.put("amount", row.getAs[Double]("amount"))
    val things = row.getAs[Seq[Row]]("things")
    val things2: List[GenericRecord] = things.toList.map { r =>
      val rec = new GenericData.Record(thingSchema)
      rec.put("name", r.getAs[String]("name"))
      rec.put("value", r.getAs[String]("value"))
      rec
    }
    rec.put("things",
      new GenericData.Array[GenericRecord](arrayOfThingsSchema, things2.asJava))
    rec
  }

  private def serialize(schema: Schema, record: GenericRecord,
                        baos: ByteArrayOutputStream): Array[Byte] = {
    val outputDatumWriter = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get.binaryEncoder(baos, null)
    outputDatumWriter.write(record, encoder)
    encoder.flush()
    baos.toByteArray
  }
}

