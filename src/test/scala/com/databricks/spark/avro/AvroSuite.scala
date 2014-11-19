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
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class AvroSuite extends FunSuite {
  val episodesFile = "src/test/resources/episodes.avro"

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
}
