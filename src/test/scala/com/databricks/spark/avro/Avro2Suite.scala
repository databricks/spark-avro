package com.databricks.spark.avro

import java.io.File
import java.sql.Timestamp

import com.google.common.io.Files
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

class Avro2Suite extends FunSuite {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"
  // A collection of random generated paritioned files from test.avro's schema
  //   11 partitions in total, 3 records per parititon, and the size of records vary.
  val testRandomPartitionedFiles = "src/test/resources/test-random-partitioned"

  test("dsl test") {
    val results = TestSQLContext.read.avro(episodesFile).select("title").collect()
    assert(results.length === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = TestSQLContext.read.avro(testFile).collect()
    assert(all.length == 3)

    val str = TestSQLContext.read.avro(testFile).select("string").collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = TestSQLContext.read.avro(testFile).select("simple_map").collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = TestSQLContext.read.avro(testFile).select("union_string_null").collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = TestSQLContext.read.avro(testFile).select("union_int_long_null").collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = TestSQLContext.read.avro(testFile).select("union_float_double").collect()
    assert(union2.map(x => new java.lang.Double(x(0).toString)).exists(p => Math.abs(p - Math.PI) < 0.001))

    val fixed = TestSQLContext.read.avro(testFile).select("fixed3").collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = TestSQLContext.read.avro(testFile).select("enum").collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = TestSQLContext.read.avro(testFile).select("record").collect()
    assert(record(0)(0).getClass.toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = TestSQLContext.read.avro(testFile).select("array_of_boolean").collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = TestSQLContext.read.avro(testFile).select("bytes").collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].length).toSet == Set(3, 1, 0))
  }

  test("sql test") {
    sql(
      s"""
         |CREATE TEMPORARY TABLE avroTable
         |USING com.databricks.spark.avro.avroRelation2
         |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM avroTable").collect().length === 8)
  }


  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    val tempDir = Files.createTempDir()
    val avroDir = s"$tempDir/avro"
    TestSQLContext.read.avro(testFile).write.avro(avroDir)
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
    TestSQLContext.read.avro(testFile).write.options(parameters).avro(avroDir)
    TestUtils.checkReloadMatchesSaved(testFile, avroDir)

    // Look at raw file and make sure has namespace info
    val rawSaved = TestSQLContext.sparkContext.textFile(avroDir)
    val schema = rawSaved.collect().mkString("")
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
    for (i <- arrayOfByte.indices) {
      arrayOfByte(i) = i.toByte
    }
    val cityRDD = sparkContext.parallelize(Seq(
      Row("San Francisco", 12, new Timestamp(666), null, arrayOfByte),
      Row("Palo Alto", null, new Timestamp(777), null, arrayOfByte),
      Row("Munich", 8, new Timestamp(42), 3.14, arrayOfByte)))
    val cityDataFrame = TestSQLContext.createDataFrame(cityRDD, testSchema)

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    cityDataFrame.write.avro(avroDir)
    assert(TestSQLContext.read.avro(avroDir).collect().length == 3)

    // TimesStamps are converted to longs
    val times = TestSQLContext.read.avro(avroDir).select("Time").collect()
    assert(times.map(_(0)).toSet == Set(666, 777, 42))

    // DecimalType should be converted to string
    val decimals = TestSQLContext.read.avro(avroDir).select("Decimal").collect()
    assert(decimals.map(_(0)).contains("3.14"))

    // There should be a null entry
    val length = TestSQLContext.read.avro(avroDir).select("Length").collect()
    assert(length.map(_(0)).contains(null))

    val binary = TestSQLContext.read.avro(avroDir).select("Binary").collect()
    for (i <- arrayOfByte.indices) {
      assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
    }

    TestUtils.deleteRecursively(tempDir)
  }

  test("support of globbed paths") {
    val e1 = TestSQLContext.read.avro("*/test/resources/episodes.avro").collect()
    assert(e1.length == 8)

    val e2 = TestSQLContext.read.avro("src/*/*/episodes.avro").collect()
    assert(e2.length == 8)
  }

  test("reading from invalid path throws exception") {
    intercept[RuntimeException] {
      TestSQLContext.read.avro("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[RuntimeException] {
      TestSQLContext.read.avro("*/*/*/*/*/*/*/something.avro")
    }
  }

  test("SQL test insert overwrite") {
    val tempDir = Files.createTempDir()
    val tempEmptyDir = s"$tempDir/empty"
    // Create a temp directory for table that will be overwritten
    new File(tempEmptyDir).mkdirs()
    sql(
      s"""
         |CREATE TEMPORARY TABLE episodes
         |USING com.databricks.spark.avro.avroRelation2
         |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))
    sql(s"""
           |CREATE TEMPORARY TABLE episodesEmpty
           |(name string, air_date string, doctor int)
           |USING com.databricks.spark.avro.avroRelation2
           |OPTIONS (path "$tempEmptyDir")
      """.stripMargin.replaceAll("\n", " "))

    assert(sql("SELECT * FROM episodes").collect().length === 8)
    assert(sql("SELECT * FROM episodesEmpty").collect().isEmpty)

    sql(
      s"""
         |INSERT OVERWRITE TABLE episodesEmpty
         |SELECT * FROM episodes
      """.stripMargin.replaceAll("\n", " "))
    assert(sql("SELECT * FROM episodesEmpty").collect().length == 8)
    TestUtils.deleteRecursively(new File(tempEmptyDir))
  }

  test("test save and load") {
    // Test if load works as expected
    val df = TestSQLContext.read.avro(episodesFile)
    assert(df.count == 8)

    val tempDir = Files.createTempDir()
    val tempSaveDir = s"$tempDir/save/"

    df.write.avro(tempSaveDir)
    val newDf = TestSQLContext.read.avro(tempSaveDir)
    assert(newDf.count == 8)
    TestUtils.deleteRecursively(tempDir)
  }
}
