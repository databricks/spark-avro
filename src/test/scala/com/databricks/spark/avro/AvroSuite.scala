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

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.sql.test._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class AvroSuite extends FunSuite {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"

  test("dsl test") {
    val results = TestSQLContext
      .avroFile(episodesFile)
      .select('title)
      .collect()

    assert(results.size === 8)
  }

  test("sql test") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE avroTable
        |USING com.databricks.spark.avro
        |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM avroTable").collect().size === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = TestSQLContext
      .avroFile(testFile)
      .collect()
    assert(all.size == 3)

    val str = TestSQLContext
      .avroFile(testFile)
      .select('string)
      .collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = TestSQLContext
      .avroFile(testFile)
      .select('simple_map)
      .collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = TestSQLContext
      .avroFile(testFile)
      .select('union_string_null)
      .collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = TestSQLContext
      .avroFile(testFile)
      .select('union_int_long_null)
      .collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = TestSQLContext
      .avroFile(testFile)
      .select('union_float_double)
      .collect()
    assert(union2.map(x => new java.lang.Double(x(0).toString)).exists(
      p => Math.abs(p - Math.PI) < 0.001))

    val fixed = TestSQLContext
      .avroFile(testFile)
      .select('fixed3)
      .collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = TestSQLContext
      .avroFile(testFile)
      .select('enum)
      .collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = TestSQLContext
      .avroFile(testFile)
      .select('record)
      .collect()
    assert(record(0)(0).getClass().toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = TestSQLContext
      .avroFile(testFile)
      .select('array_of_boolean)
      .collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = TestSQLContext
      .avroFile(testFile)
      .select('bytes)
      .collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].size).toSet == Set(3, 1, 0))
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.

    def convertToString(elem: Any): String = {
      elem match {
        case null => "NULL" // HashSets can't have null in them, so we use a string instead
        case arrayBuf: ArrayBuffer[Any] => arrayBuf.toArray.deep.mkString(" ")
        case arrayByte: Array[Byte] => arrayByte.deep.mkString(" ")
        case other => other.toString
      }
    }

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    AvroSaver.save(TestSQLContext.avroFile(testFile), avroDir)

    val originalEntries = TestSQLContext.avroFile(testFile).collect()
    val newEntries = TestSQLContext.avroFile(avroDir).collect()

    assert(originalEntries.size == newEntries.size)

    val origEntrySet = new Array[HashSet[Any]](originalEntries(0).size)
    for (i <- 0 until originalEntries(0).size) {origEntrySet(i) = new HashSet[Any]()}

    for (origEntry <- originalEntries) {
      var idx = 0
      for (origElement <- origEntry) {
        origEntrySet(idx) += convertToString(origElement)
        idx += 1
      }
    }

    for (newEntry <- newEntries) {
      var idx = 0
      for (newElement <- newEntry) {
        assert(origEntrySet(idx).contains(convertToString(newElement)))
        idx += 1
      }
    }

    FileUtils.deleteDirectory(tempDir)
  }

  test("converting some specific sparkSQL types to avro") {
    val testSchema = StructType(Seq(StructField("Name", StringType, false),
                                    StructField("Length", IntegerType, true),
                                    StructField("Time", TimestampType, false),
                                    StructField("Decimal", DecimalType(10, 10), true),
                                    StructField("Binary", BinaryType, false)))

    val arrayOfByte = new Array[Byte](4)
    for (i <- 0 until arrayOfByte.size) {
      arrayOfByte(i) = i.toByte
    }
    val cityRDD = sparkContext.parallelize(Seq(
      Row("San Francisco", 12, new Timestamp(666), null, arrayOfByte),
      Row("Palo Alto", null, new Timestamp(777), null, arrayOfByte),
      Row("Munich", 8, new Timestamp(42), 3.14, arrayOfByte)))
    val citySchemaRDD = TestSQLContext.applySchema(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    AvroSaver.save(citySchemaRDD, avroDir)

    assert(TestSQLContext.avroFile(avroDir).collect().size == 3)

    // TimesStamps are converted to longs
    val times = TestSQLContext
      .avroFile(avroDir)
      .select('Time)
      .collect()
    assert(times.map(_(0)).toSet == Set(666, 777, 42))

    // DecimalType should be converted to string
    val decimals = TestSQLContext
      .avroFile(avroDir)
      .select('Decimal)
      .collect()
    assert(decimals.map(_(0)).contains("3.14"))

    // There should be a null entry
    val length = TestSQLContext
      .avroFile(avroDir)
      .select('Length)
      .collect()
    assert(length.map(_(0)).contains(null))

    val binary = TestSQLContext
      .avroFile(avroDir)
      .select('Binary)
      .collect()
    for (i <- 0 until arrayOfByte.size) {
      assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
    }

    FileUtils.deleteDirectory(tempDir)
  }

  test("support of globbed paths") {
    val e1 = TestSQLContext
      .avroFile("*/test/resources/episodes.avro")
      .collect()
    assert(e1.size == 8)

    val e2 = TestSQLContext
      .avroFile("src/*/*/episodes.avro")
      .collect()
    assert(e2.size == 8)
  }
}
