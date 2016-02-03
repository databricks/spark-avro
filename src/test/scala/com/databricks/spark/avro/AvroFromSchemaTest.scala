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

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AvroFromSchemaTest extends FlatSpec with BeforeAndAfter with Matchers {

  var tempDir: File = _
  var sc: SparkContext = _
  var avroSchema1: Schema = _
  var avroSchema2: Schema = _

  before {
    def writeAvroFile(fileName: String, schema: Schema, user: GenericRecord) = {
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(fileName))
      dataFileWriter.append(user)
      dataFileWriter.close()
    }

    avroSchema1 = new Schema.Parser().parse(
      new File("src/test/resources/avro-evolution/user1.avsc"))
    avroSchema2 = new Schema.Parser().parse(
      new File("src/test/resources/avro-evolution/user2.avsc"))

    // record with schema #1
    val user1 = new GenericData.Record(avroSchema1)
    user1.put("name", "Alyssa")
    user1.put("favorite_number", 256)

    // record with schema #2
    val user2 = new GenericData.Record(avroSchema2)
    user2.put("name", "Ben")
    user2.put("favorite_number", 7)
    // schema #2 has this new `favorite_color` field
    user2.put("favorite_color", "red")

    val baseTmp = System.getProperty("java.io.tmpdir")
    val testSuffix = s"spark-avro-test-${System.currentTimeMillis()}"
    tempDir = new File(baseTmp, testSuffix)
    tempDir.mkdir()
    writeAvroFile(s"${tempDir.getAbsolutePath}/user1.avro", avroSchema1, user1)
    writeAvroFile(s"${tempDir.getAbsolutePath}/user2.avro", avroSchema2, user2)

    val conf = new SparkConf() .setMaster("local[2]") .setAppName("app")

    sc = new SparkContext(conf)
  }

  after {
    FileUtils.deleteDirectory(tempDir)
    sc.stop()
  }

  "spark-avro" should "be able to read data according to a specific avro Schema" in {
    case class User1(name: String, favorite_number: Int)
    case class User2(name: String, favorite_number: Int, favorite_color: String)

    val sqlContext = new SQLContext(sc)

    // read both files with schema only has two fields
    val df = sqlContext
      .read
      .schema(avroSchema1)
      .avro(s"${tempDir.getAbsolutePath}/")

    val dumpedData = df
      .collect()
      .map(row => User1(row.getString(0), row.getInt(1)))

    dumpedData.toList should be(List(User1("Alyssa", 256), User1("Ben", 7)))

    // read both files with schema has three fields
    val df2 = sqlContext
      .read
      .schema(avroSchema2)
      .avro(s"${tempDir.getAbsolutePath}/")

    val dumpedData2 = df2
      .collect()
      .map(row => User2(row.getString(0), row.getInt(1), row.getString(2)))

    dumpedData2.toList should be(List(User2("Alyssa", 256, null), User2("Ben", 7, "red")))
  }
}
