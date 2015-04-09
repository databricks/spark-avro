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

import java.io.FileNotFoundException
import java.io.File
import java.io.IOException
import java.sql.Timestamp
import java.util.{ArrayList, HashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import com.google.common.io.Files
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.util.Random

/* Implicits */
import TestSQLContext._

private[avro] object TestUtils {

  /**
   * This function checks that all records in a file match the original
   * record.
   */

  def checkReloadMatchesSaved(testFile: String, avroDir: String) = {

    def convertToString(elem: Any): String = {
      elem match {
        case null => "NULL" // HashSets can't have null in them, so we use a string instead
        case arrayBuf: ArrayBuffer[Any] => arrayBuf.toArray.deep.mkString(" ")
        case arrayByte: Array[Byte] => arrayByte.deep.mkString(" ")
        case other => other.toString
      }
    }

    val originalEntries = TestSQLContext.avroFile(testFile).collect()
    val newEntries = TestSQLContext.avroFile(avroDir).collect()

    assert(originalEntries.size == newEntries.size)

    val origEntrySet = new Array[HashSet[Any]](originalEntries(0).size)
    for (i <- 0 until originalEntries(0).size) { origEntrySet(i) = new HashSet[Any]() }

    for (origEntry <- originalEntries) {
      var idx = 0
      for (origElement <- origEntry.toSeq) {
        origEntrySet(idx) += convertToString(origElement)
        idx += 1
      }
    }

    for (newEntry <- newEntries) {
      var idx = 0
      for (newElement <- newEntry.toSeq) {
        assert(origEntrySet(idx).contains(convertToString(newElement)))
        idx += 1
      }
    }
  }

  /**
   * This function deletes a file or a directory with everything that's in it. This function is
   * copied from Spark with minor modifications made to it. See original source at:
   * github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
   */

  def deleteRecursively(file: File) {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * This function generates a random map(string, int) of a given size.
   */
  private[avro] def generateRandomMap(rand: Random, size: Int): java.util.Map[String, Int] = {
    val jMap = new HashMap[String, Int]()
    for (i <- 0 until size) {
      jMap.put(rand.nextString(5), i)
    }
    jMap
  }

  /**
   * This function generates a random array of booleans of a given size.
   */
  private[avro] def generateRandomArray(rand: Random, size: Int): ArrayList[Boolean] = {
    val vec = new ArrayList[Boolean]()
    for (i <- 0 until size) {
      vec.add(rand.nextBoolean())
    }
    vec
  }
}

class AvroSuite extends FunSuite {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"
  // A collection of random generated paritioned files from test.avro's schema
  //   11 partitions in total, 3 records per parititon, and the size of records vary.
  val testRandomPartitionedFiles = "src/test/resources/test-random-partitioned"

  test("dsl test") {
    val results = TestSQLContext
      .avroFile(episodesFile)
      .select("title")
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
      .select("string")
      .collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = TestSQLContext
      .avroFile(testFile)
      .select("simple_map")
      .collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = TestSQLContext
      .avroFile(testFile)
      .select("union_string_null")
      .collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = TestSQLContext
      .avroFile(testFile)
      .select("union_int_long_null")
      .collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = TestSQLContext
      .avroFile(testFile)
      .select("union_float_double")
      .collect()
    assert(union2.map(x => new java.lang.Double(x(0).toString)).exists(
      p => Math.abs(p - Math.PI) < 0.001))

    val fixed = TestSQLContext
      .avroFile(testFile)
      .select("fixed3")
      .collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = TestSQLContext
      .avroFile(testFile)
      .select("enum")
      .collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = TestSQLContext
      .avroFile(testFile)
      .select("record")
      .collect()
    assert(record(0)(0).getClass().toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = TestSQLContext
      .avroFile(testFile)
      .select("array_of_boolean")
      .collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = TestSQLContext
      .avroFile(testFile)
      .select("bytes")
      .collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].size).toSet == Set(3, 1, 0))
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    AvroSaver.save(TestSQLContext.avroFile(testFile), avroDir)

    TestUtils.checkReloadMatchesSaved(testFile, avroDir)
    TestUtils.deleteRecursively(tempDir)
  }

  test("conversion to avro and back with namespace") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    val name = "AvroTest"
    val namespace = "com.databricks.spark.avro"
    val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/namedAvro"
    AvroSaver.save(TestSQLContext.avroFile(testFile), avroDir, parameters)

    TestUtils.checkReloadMatchesSaved(testFile, avroDir)

    // Look at raw file and make sure has namespace info
    val rawSaved = TestSQLContext.sparkContext.textFile(avroDir)
    val schema = rawSaved.first()
    assert(schema.contains(name))
    assert(schema.contains(namespace))

    TestUtils.deleteRecursively(tempDir)
  }

  test("converting some specific sparkSQL types to avro") {
    val testSchema = StructType(Seq(
      StructField("Name", StringType, false),
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
    val cityDataFrame = TestSQLContext.applySchema(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    AvroSaver.save(cityDataFrame, avroDir)

    assert(TestSQLContext.avroFile(avroDir).collect().size == 3)

    // TimesStamps are converted to longs
    val times = TestSQLContext
      .avroFile(avroDir)
      .select("Time")
      .collect()
    assert(times.map(_(0)).toSet == Set(666, 777, 42))

    // DecimalType should be converted to string
    val decimals = TestSQLContext
      .avroFile(avroDir)
      .select("Decimal")
      .collect()
    assert(decimals.map(_(0)).contains("3.14"))

    // There should be a null entry
    val length = TestSQLContext
      .avroFile(avroDir)
      .select("Length")
      .collect()
    assert(length.map(_(0)).contains(null))

    val binary = TestSQLContext
      .avroFile(avroDir)
      .select("Binary")
      .collect()
    for (i <- 0 until arrayOfByte.size) {
      assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
    }

    TestUtils.deleteRecursively(tempDir)
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

  test("reading from invalid path throws exception") {
    intercept[FileNotFoundException] {
      TestSQLContext.avroFile("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[RuntimeException] {
      TestSQLContext.avroFile("*/*/*/*/*/*/*/something.avro")
    }
  }

  test("reading with different partitions number choices") {
    def getNumPartitions(df: DataFrame): Long = {
      df.mapPartitions((it) => it).partitions.length
    }
    val defaultMinPartitions = TestSQLContext.sparkContext.defaultMinPartitions

    // Read a single file, and use the default partitions number
    val e1 = TestSQLContext
      .avroFile(testFile)
    assert(getNumPartitions(e1) == defaultMinPartitions)

    // Read a single file, and set the partitions number larger than file number
    val e2 = TestSQLContext
      .avroFile(testFile, 12)
    assert(getNumPartitions(e2) == 12)

    // Read a single file, provide a invalid partitions number (negative one)
    val e3 = TestSQLContext
      .avroFile(testFile, -9)
    assert(getNumPartitions(e3) == defaultMinPartitions)

    // Read muitple files, and use the default partitions number
    val e4 = TestSQLContext
      .avroFile(testRandomPartitionedFiles)
    assert(getNumPartitions(e4) == 11)

    // Read multiple files, and set the partitions number smaller than file number
    val e5 = TestSQLContext
      .avroFile(testRandomPartitionedFiles, 1)
    assert(getNumPartitions(e5) == 11)

    // Read multiple files, and set the partitions number larger than file number
    // But there is no guarantee what is the exact number of partitions because when record size
    // varies. We only know it should be approximately larger than the specified number.
    val e6 = TestSQLContext
      .avroFile(testRandomPartitionedFiles, 22)
    assert(getNumPartitions(e6) >= 22)

    val e7 = TestSQLContext
      .avroFile(testRandomPartitionedFiles, 110)
    assert(getNumPartitions(e7) >= 110)
  }

  test("SQL test insert overwrite") {
    val tempEmptyDir = "target/test/empty/"
    // Create a temp directory for table that will be overwritten
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    sql(
      s"""
        |CREATE TEMPORARY TABLE episodes
        |USING com.databricks.spark.avro
        |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))
    sql(s"""
        |CREATE TEMPORARY TABLE episodesEmpty
        |(name string, air_date string, doctor int)
        |USING com.databricks.spark.avro
        |OPTIONS (path "$tempEmptyDir")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM episodes").collect().size === 8)
    assert(sql("SELECT * FROM episodesEmpty").collect().isEmpty)

    sql(
      s"""
        |INSERT OVERWRITE TABLE episodesEmpty
        |SELECT * FROM episodes
      """.stripMargin.replaceAll("\n", " "))
    assert(sql("SELECT * FROM episodesEmpty").collect().size == 8)
  }

  test("test save and load") {
    // Test if load works as expected
    val df = TestSQLContext.load(episodesFile, "com.databricks.spark.avro")
    assert(df.count == 8)

    // Test if save works as expected
    val tempSaveDir = "target/test/save/"
    TestUtils.deleteRecursively(new File(tempSaveDir))
    df.save(tempSaveDir, "com.databricks.spark.avro")
    val newDf = TestSQLContext.load(tempSaveDir, "com.databricks.spark.avro")
    assert(newDf.count == 8)
  }

}
