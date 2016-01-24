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

import java.nio.file.{Files, Path}

import com.databricks.spark.avro.example.User
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AvroFromSchemaTest extends FlatSpec with BeforeAndAfter with Matchers {

  var tempDir: Path = _
  var sc: SparkContext

  before {
    tempDir = Files.createTempDirectory("spark-avro-test")
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("app")
      .registerKryoClasses(Array(classOf[User]))

    sc = new SparkContext(conf)
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
    sc.stop()
  }

  "spark-avro" should "be able to read/write data according to a specific avro Schema" in {
    val sqlContext = new SQLContext(sc)
    val users = List(new User("my_name", 13, "green"))
    val df = sc.parallelize(users)
      .toAvroDF(sqlContext)

    val computedSchema = df.schema

    df
      .write
      .option("avro.output.schema.class", classOf[User].getCanonicalName)
      .avro(s"${tempDir.toAbsolutePath}/test")

    val ddf = sqlContext
      .read
      .option("avro.input.schema.class", classOf[User].getCanonicalName)
      .avro(s"${tempDir.toAbsolutePath}/test")

    ddf.schema should be (computedSchema)

    val dumpedData = ddf
      .map(row => new User(row.getString(0), row.getInt(1), row.getString(2)))
      .collect()

    dumpedData.toList should be (users)
  }
}
