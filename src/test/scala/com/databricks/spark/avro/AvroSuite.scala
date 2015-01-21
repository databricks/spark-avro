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

import org.apache.spark.sql.test._
import org.apache.spark.sql.Row
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
    assert(str(1)(0) == "Terran is IMBA!")

    val simple_map = TestSQLContext
      .avroFile(testFile)
      .select('simple_map)
      .collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map(0)(0).asInstanceOf[Map[String, Some[Int]]].get("abc") == Some[Int](1))

    val union0 = TestSQLContext
      .avroFile(testFile)
      .select('union_string_null)
      .collect()
    assert(union0(0)(0) == "abc")
    assert(union0(1)(0) == "123")
    assert(union0(2)(0) == null)

    val union1 = TestSQLContext
      .avroFile(testFile)
      .select('union_int_long_null)
      .collect()
    assert(union1(1)(0) == 66)
    assert(union1(2)(0) == null)

    val union2 = TestSQLContext
      .avroFile(testFile)
      .select('union_float_double)
      .collect()
    assert(Math.abs(union2(0)(0).asInstanceOf[Float] - Math.PI) < 0.001)

    val fixed = TestSQLContext
      .avroFile(testFile)
      .select('fixed3)
      .collect()
    assert(fixed(0)(0).asInstanceOf[Array[Byte]](1) == 3)

    val enum = TestSQLContext
      .avroFile(testFile)
      .select('enum)
      .collect()
    assert(enum(0)(0) == "SPADES")
    assert(enum(1)(0) == "CLUBS")

    val record = TestSQLContext
      .avroFile(testFile)
      .select('record)
      .collect()
    assert(record(0)(0).getClass().toString.contains("Row"))
    assert(record(2)(0).asInstanceOf[Row](0) == "TEST_STR123")

    val array_of_boolean = TestSQLContext
      .avroFile(testFile)
      .select('array_of_boolean)
      .collect()
    assert(array_of_boolean(0)(0).asInstanceOf[Seq[Boolean]](0))
    assert(!array_of_boolean(0)(0).asInstanceOf[Seq[Boolean]](1))
    assert(!array_of_boolean(0)(0).asInstanceOf[Seq[Boolean]](2))
    assert(!array_of_boolean(2)(0).asInstanceOf[Seq[Boolean]](0))

    val bytes = TestSQLContext
      .avroFile(testFile)
      .select('bytes)
      .collect()
    assert(bytes(0)(0).asInstanceOf[Array[Byte]](0) == 65)
    assert(bytes(0)(0).asInstanceOf[Array[Byte]](1) == 66)
    assert(bytes(2)(0).asInstanceOf[Array[Byte]](0) == 83)
  }
}
