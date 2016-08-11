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

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.sql.Timestamp
import java.util.UUID

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AvroSuite extends FunSuite with BeforeAndAfterAll {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[2]").appName("AvroSuite").getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("reading and writing partitioned data") {
    val df = spark.read.avro(episodesFile)
    val fields = List("title", "air_date", "doctor")
    for (field <- fields) {
      TestUtils.withTempDir { dir =>
        val outputDir = s"$dir/${UUID.randomUUID}"
        df.write.partitionBy(field).avro(outputDir)
        val input = spark.read.avro(outputDir)
        // makes sure that no fields got dropped.
        // We convert Rows to Seqs in order to work around SPARK-10325
        assert(input.select(field).collect().map(_.toSeq).toSet ===
          df.select(field).collect().map(_.toSeq).toSet)
      }
    }
  }

  test("request no fields") {
    val df = spark.read.avro(episodesFile)
    df.registerTempTable("avro_table")
    assert(spark.sql("select count(*) from avro_table").collect().head === Row(8))
  }

  test("convert formats") {
    TestUtils.withTempDir { dir =>
      val df = spark.read.avro(episodesFile)
      df.write.parquet(dir.getCanonicalPath)
      assert(spark.read.parquet(dir.getCanonicalPath).count() === df.count)
    }
  }

  test("rearrange internal schema") {
    TestUtils.withTempDir { dir =>
      val df = spark.read.avro(episodesFile)
      df.select("doctor", "title").write.avro(dir.getCanonicalPath)
    }
  }

  test("test NULL avro type") {
    TestUtils.withTempDir { dir =>
      val fields = Seq(new Field("null", Schema.create(Type.NULL), "doc", null))
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)
      avroRec.put("null", null)
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()
      intercept[UnsupportedOperationException] {
        spark.read.avro(s"$dir.avro")
      }
    }
  }

  test("Union of a single type") {
    TestUtils.withTempDir { dir =>
      val UnionOfOne = Schema.createUnion(List(Schema.create(Type.INT)))
      val fields = Seq(new Field("field1", UnionOfOne, "doc", null))
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)

      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)

      avroRec.put("field1", 8)

      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()

      val df = spark.read.avro(s"$dir.avro")
      assert(df.first() == Row(8))
    }
  }

  test("Incorrect Union Type") {
    TestUtils.withTempDir { dir =>
      val BadUnionType = Schema.createUnion(
        List(Schema.create(Type.INT), Schema.create(Type.STRING)))
      val fixedSchema = Schema.createFixed("fixed_name", "doc", "namespace", 20)
      val fixedUnionType = Schema.createUnion(List(fixedSchema,Schema.create(Type.NULL)))
      val fields = Seq(new Field("field1", BadUnionType, "doc", null),
        new Field("fixed", fixedUnionType, "doc", null),
        new Field("bytes", Schema.create(Type.BYTES), "doc", null))
      val schema = Schema.createRecord("name", "docs", "namespace", false)
      schema.setFields(fields)
      val datumWriter = new GenericDatumWriter[GenericRecord](schema)
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, new File(s"$dir.avro"))
      val avroRec = new GenericData.Record(schema)
      avroRec.put("field1", "Hope that was not load bearing")
      avroRec.put("bytes", ByteBuffer.wrap(Array[Byte]()))
      dataFileWriter.append(avroRec)
      dataFileWriter.flush()
      dataFileWriter.close()
      intercept[UnsupportedOperationException] {
        spark.read.avro(s"$dir.avro")
      }
    }
  }

  test("Lots of nulls") {
    TestUtils.withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("binary", BinaryType, true),
        StructField("timestamp", TimestampType, true),
        StructField("array", ArrayType(ShortType), true),
        StructField("map", MapType(StringType, StringType), true),
        StructField("struct", StructType(Seq(StructField("int", IntegerType, true))))))
      val rdd = spark.sparkContext.parallelize(Seq[Row](
        Row(null, new Timestamp(1), Array[Short](1,2,3), null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null)))
      val df = spark.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("Struct field type") {
    TestUtils.withTempDir { dir =>
      val schema = StructType(Seq(
        StructField("float", FloatType, true),
        StructField("short", ShortType, true),
        StructField("byte", ByteType, true),
        StructField("boolean", BooleanType, true)
      ))
      val rdd = spark.sparkContext.parallelize(Seq(
        Row(1f, 1.toShort, 1.toByte, true),
        Row(2f, 2.toShort, 2.toByte, true),
        Row(3f, 3.toShort, 3.toByte, true)
      ))
      val df = spark.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("Array data types") {
    TestUtils.withTempDir { dir =>
      val testSchema = StructType(Seq(
        StructField("byte_array", ArrayType(ByteType), true),
        StructField("short_array", ArrayType(ShortType), true),
        StructField("float_array", ArrayType(FloatType), true),
        StructField("bool_array", ArrayType(BooleanType), true),
        StructField("long_array", ArrayType(LongType), true),
        StructField("double_array", ArrayType(DoubleType), true),
        StructField("decimal_array", ArrayType(DecimalType(10, 0)), true),
        StructField("bin_array", ArrayType(BinaryType), true),
        StructField("timestamp_array", ArrayType(TimestampType), true),
        StructField("array_array", ArrayType(ArrayType(StringType), true), true),
        StructField("struct_array", ArrayType(
          StructType(Seq(StructField("name", StringType, true)))))))

      val arrayOfByte = new Array[Byte](4)
      for (i <- arrayOfByte.indices) {
        arrayOfByte(i) = i.toByte
      }

      val rdd = spark.sparkContext.parallelize(Seq(
        Row(arrayOfByte, Array[Short](1,2,3,4), Array[Float](1f, 2f, 3f, 4f),
          Array[Boolean](true, false, true, false), Array[Long](1L, 2L), Array[Double](1.0, 2.0),
          Array[BigDecimal](BigDecimal.valueOf(3)), Array[Array[Byte]](arrayOfByte, arrayOfByte),
          Array[Timestamp](new Timestamp(0)),
          Array[Array[String]](Array[String]("CSH, tearing down the walls that divide us", "-jd")),
          Array[Row](Row("Bobby G. can't swim")))))
      val df = spark.createDataFrame(rdd, testSchema)
      df.write.avro(dir.toString)
      assert(spark.read.avro(dir.toString).count == rdd.count)
    }
  }

  test("write with compression") {
    TestUtils.withTempDir { dir =>
      val AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
      val AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level"
      val uncompressDir = s"$dir/uncompress"
      val deflateDir = s"$dir/deflate"
      val snappyDir = s"$dir/snappy"
      val fakeDir = s"$dir/fake"

      val df = spark.read.avro(testFile)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "uncompressed")
      df.write.avro(uncompressDir)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "deflate")
      spark.conf.set(AVRO_DEFLATE_LEVEL, "9")
      df.write.avro(deflateDir)
      spark.conf.set(AVRO_COMPRESSION_CODEC, "snappy")
      df.write.avro(snappyDir)

      val uncompressSize = FileUtils.sizeOfDirectory(new File(uncompressDir))
      val deflateSize = FileUtils.sizeOfDirectory(new File(deflateDir))
      val snappySize = FileUtils.sizeOfDirectory(new File(snappyDir))

      assert(uncompressSize > deflateSize)
      assert(snappySize > deflateSize)
    }
  }

  test("dsl test") {
    val results = spark.read.avro(episodesFile).select("title").collect()
    assert(results.length === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = spark.read.avro(testFile).collect()
    assert(all.length == 3)

    val str = spark.read.avro(testFile).select("string").collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = spark.read.avro(testFile).select("simple_map").collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = spark.read.avro(testFile).select("union_string_null").collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = spark.read.avro(testFile).select("union_int_long_null").collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = spark.read.avro(testFile).select("union_float_double").collect()
    assert(
      union2
        .map(x => new java.lang.Double(x(0).toString))
        .exists(p => Math.abs(p - Math.PI) < 0.001))

    val fixed = spark.read.avro(testFile).select("fixed3").collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = spark.read.avro(testFile).select("enum").collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = spark.read.avro(testFile).select("record").collect()
    assert(record(0)(0).getClass.toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = spark.read.avro(testFile).select("array_of_boolean").collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = spark.read.avro(testFile).select("bytes").collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].length).toSet == Set(3, 1, 0))
  }

  test("sql test") {
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE avroTable
         |USING com.databricks.spark.avro
         |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT * FROM avroTable").collect().length === 8)
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    TestUtils.withTempDir { dir =>
      val avroDir = s"$dir/avro"
      spark.read.avro(testFile).write.avro(avroDir)
      TestUtils.checkReloadMatchesSaved(spark, testFile, avroDir)
    }
  }

  test("conversion to avro and back with namespace") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    TestUtils.withTempDir { tempDir =>
      val name = "AvroTest"
      val namespace = "com.databricks.spark.avro"
      val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

      val avroDir = tempDir + "/namedAvro"
      spark.read.avro(testFile).write.options(parameters).avro(avroDir)
      TestUtils.checkReloadMatchesSaved(spark, testFile, avroDir)

      // Look at raw file and make sure has namespace info
      val rawSaved = spark.sparkContext.textFile(avroDir)
      val schema = rawSaved.collect().mkString("")
      assert(schema.contains(name))
      assert(schema.contains(namespace))
    }
  }

  test("converting some specific sparkSQL types to avro") {
    TestUtils.withTempDir { tempDir =>
      val testSchema = StructType(Seq(
        StructField("Name", StringType, false),
        StructField("Length", IntegerType, true),
        StructField("Time", TimestampType, false),
        StructField("Decimal", DecimalType(10, 2), true),
        StructField("Binary", BinaryType, false)))

      val arrayOfByte = new Array[Byte](4)
      for (i <- arrayOfByte.indices) {
        arrayOfByte(i) = i.toByte
      }
      val cityRDD = spark.sparkContext.parallelize(Seq(
        Row("San Francisco", 12, new Timestamp(666), null, arrayOfByte),
        Row("Palo Alto", null, new Timestamp(777), null, arrayOfByte),
        Row("Munich", 8, new Timestamp(42), Decimal(3.14), arrayOfByte)))
      val cityDataFrame = spark.createDataFrame(cityRDD, testSchema)

      val avroDir = tempDir + "/avro"
      cityDataFrame.write.avro(avroDir)
      assert(spark.read.avro(avroDir).collect().length == 3)

      // TimesStamps are converted to longs
      val times = spark.read.avro(avroDir).select("Time").collect()
      assert(times.map(_(0)).toSet == Set(666, 777, 42))

      // DecimalType should be converted to string
      val decimals = spark.read.avro(avroDir).select("Decimal").collect()
      assert(decimals.map(_(0)).contains("3.14"))

      // There should be a null entry
      val length = spark.read.avro(avroDir).select("Length").collect()
      assert(length.map(_(0)).contains(null))

      val binary = spark.read.avro(avroDir).select("Binary").collect()
      for (i <- arrayOfByte.indices) {
        assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
      }
    }
  }

  test("support of globbed paths") {
    val e1 = spark.read.avro("*/test/resources/episodes.avro").collect()
    assert(e1.length == 8)

    val e2 = spark.read.avro("src/*/*/episodes.avro").collect()
    assert(e2.length == 8)
  }

  test("reading from invalid path throws exception") {

    // Directory given has no avro files
    intercept[AnalysisException] {
      TestUtils.withTempDir(dir => spark.read.avro(dir.getCanonicalPath))
    }

    intercept[AnalysisException] {
      spark.read.avro("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[AnalysisException] {
      spark.read.avro("*/*/*/*/*/*/*/something.avro")
    }

    intercept[FileNotFoundException] {
      TestUtils.withTempDir { dir =>
        FileUtils.touch(new File(dir, "test"))
        spark.read.avro(dir.toString)
      }
    }

  }

  test("SQL test insert overwrite") {
    TestUtils.withTempDir { tempDir =>
      val tempEmptyDir = s"$tempDir/sqlOverwrite"
      // Create a temp directory for table that will be overwritten
      new File(tempEmptyDir).mkdirs()
      spark.sql(
        s"""
           |CREATE TEMPORARY TABLE episodes
           |USING com.databricks.spark.avro
           |OPTIONS (path "$episodesFile")
         """.stripMargin.replaceAll("\n", " "))
      spark.sql(
        s"""
           |CREATE TEMPORARY TABLE episodesEmpty
           |(name string, air_date string, doctor int)
           |USING com.databricks.spark.avro
           |OPTIONS (path "$tempEmptyDir")
         """.stripMargin.replaceAll("\n", " "))

      assert(spark.sql("SELECT * FROM episodes").collect().length === 8)
      assert(spark.sql("SELECT * FROM episodesEmpty").collect().isEmpty)

      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE episodesEmpty
           |SELECT * FROM episodes
         """.stripMargin.replaceAll("\n", " "))
      assert(spark.sql("SELECT * FROM episodesEmpty").collect().length == 8)
    }
  }

  test("test save and load") {
    // Test if load works as expected
    TestUtils.withTempDir { tempDir =>
      val df = spark.read.avro(episodesFile)
      assert(df.count == 8)

      val tempSaveDir = s"$tempDir/save/"

      df.write.avro(tempSaveDir)
      val newDf = spark.read.avro(tempSaveDir)
      assert(newDf.count == 8)
    }
  }

  test("test load with non-Avro file") {
    // Test if load works as expected
    TestUtils.withTempDir { tempDir =>
      val df = spark.read.avro(episodesFile)
      assert(df.count == 8)

      val tempSaveDir = s"$tempDir/save/"
      df.write.avro(tempSaveDir)

      Files.createFile(new File(tempSaveDir, "non-avro").toPath)

      val newDf = spark
        .read
        .option(DefaultSource.IgnoreFilesWithoutExtensionProperty, "true")
        .avro(tempSaveDir)

      assert(newDf.count == 8)
    }
  }
}
