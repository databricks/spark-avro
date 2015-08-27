package com.databricks.spark.avro

import java.io.{FileNotFoundException, File}
import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.UUID

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.Schema.{Type, Field}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord, GenericDatumWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AvroSuite extends FunSuite with BeforeAndAfterAll {
  val episodesFile = "src/test/resources/episodes.avro"
  val testFile = "src/test/resources/test.avro"

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "AvroSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("reading and writing partitioned data") {
    val df = sqlContext.read.avro(episodesFile)
    val fields = List("title", "air_date", "doctor")
    for (field <- fields) {
      TestUtils.withTempDir { dir =>
        val outputDir = s"$dir/${UUID.randomUUID}"
        df.write.partitionBy(field).avro(outputDir)
        val input = sqlContext.read.avro(outputDir)
        // makes sure that no fields got dropped
        assert(input.select(field).collect().map(_(0).asInstanceOf[String]).sorted === df.select(field).collect().map(_(0).asInstanceOf[String]).sorted)
        assert(input.select(field).collect().toSet === df.select(field).collect().toSet)
      }
    }
  }

  test("request no fields") {
    val df = sqlContext.read.avro(episodesFile)
    df.registerTempTable("avro_table")
    assert(sqlContext.sql("select count(*) from avro_table").collect().head === Row(8))
  }

  test("convert formats") {
    TestUtils.withTempDir { dir =>
      val df = sqlContext.read.avro(episodesFile)
      df.write.parquet(dir.getCanonicalPath)
      assert(sqlContext.read.parquet(dir.getCanonicalPath).count() === df.count)
    }
  }

  test("rearrange internal schema") {
    TestUtils.withTempDir { dir =>
      val df = sqlContext.read.avro(episodesFile)
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
        sqlContext.read.avro(s"$dir.avro")
      }
    }
  }

  test("Incorrect Union Type") {
    TestUtils.withTempDir { dir =>
      val BadUnionType = Schema.createUnion(List(Schema.create(Type.INT),Schema.create(Type.STRING)))
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
        sqlContext.read.avro(s"$dir.avro")
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
      val rdd = sqlContext.sparkContext.parallelize(Seq[Row](
        Row(null, new Timestamp(1), Array[Short](1,2,3), null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null),
        Row(null, null, null, null, null)))
      val df = sqlContext.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(sqlContext.read.avro(dir.toString).count == rdd.count)
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
      val rdd = sqlContext.sparkContext.parallelize(Seq(
        Row(1f, 1.toShort, 1.toByte, true),
        Row(2f, 2.toShort, 2.toByte, true),
        Row(3f, 3.toShort, 3.toByte, true)
      ))
      val df = sqlContext.createDataFrame(rdd, schema)
      df.write.avro(dir.toString)
      assert(sqlContext.read.avro(dir.toString).count == rdd.count)
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
        StructField("struct_array", ArrayType(StructType(Seq(StructField("name", StringType, true)))))))

      val arrayOfByte = new Array[Byte](4)
      for (i <- arrayOfByte.indices) {
        arrayOfByte(i) = i.toByte
      }

      val rdd = sqlContext.sparkContext.parallelize(Seq(
        Row(arrayOfByte, Array[Short](1,2,3,4), Array[Float](1f, 2f, 3f, 4f),
          Array[Boolean](true, false, true, false), Array[Long](1L, 2L), Array[Double](1.0, 2.0),
          Array[BigDecimal](BigDecimal.valueOf(3)), Array[Array[Byte]](arrayOfByte, arrayOfByte),
          Array[Timestamp](new Timestamp(0)),
          Array[Array[String]](Array[String]("CSH, tearing down the walls that divide us", "-jd")),
          Array[Row](Row("Bobby G. can't swim")))))
      val df = sqlContext.createDataFrame(rdd, testSchema)
      df.write.avro(dir.toString)
      assert(sqlContext.read.avro(dir.toString).count == rdd.count)
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

      val df = sqlContext.read.avro(testFile)
      sqlContext.setConf(AVRO_COMPRESSION_CODEC, "uncompressed")
      df.write.avro(uncompressDir)
      sqlContext.setConf(AVRO_COMPRESSION_CODEC, "deflate")
      sqlContext.setConf(AVRO_DEFLATE_LEVEL, "9")
      df.write.avro(deflateDir)
      sqlContext.setConf(AVRO_COMPRESSION_CODEC, "snappy")
      df.write.avro(snappyDir)

      val uncompressSize = FileUtils.sizeOfDirectory(new File(uncompressDir))
      val deflateSize = FileUtils.sizeOfDirectory(new File(deflateDir))
      val snappySize = FileUtils.sizeOfDirectory(new File(snappyDir))

      assert(uncompressSize > deflateSize)
      assert(snappySize > deflateSize)
    }
  }

  test("dsl test") {
    val results = sqlContext.read.avro(episodesFile).select("title").collect()
    assert(results.length === 8)
  }

  test("support of various data types") {
    // This test uses data from test.avro. You can see the data and the schema of this file in
    // test.json and test.avsc
    val all = sqlContext.read.avro(testFile).collect()
    assert(all.length == 3)

    val str = sqlContext.read.avro(testFile).select("string").collect()
    assert(str.map(_(0)).toSet.contains("Terran is IMBA!"))

    val simple_map = sqlContext.read.avro(testFile).select("simple_map").collect()
    assert(simple_map(0)(0).getClass.toString.contains("Map"))
    assert(simple_map.map(_(0).asInstanceOf[Map[String, Some[Int]]].size).toSet == Set(2, 0))

    val union0 = sqlContext.read.avro(testFile).select("union_string_null").collect()
    assert(union0.map(_(0)).toSet == Set("abc", "123", null))

    val union1 = sqlContext.read.avro(testFile).select("union_int_long_null").collect()
    assert(union1.map(_(0)).toSet == Set(66, 1, null))

    val union2 = sqlContext.read.avro(testFile).select("union_float_double").collect()
    assert(union2.map(x => new java.lang.Double(x(0).toString)).exists(p => Math.abs(p - Math.PI) < 0.001))

    val fixed = sqlContext.read.avro(testFile).select("fixed3").collect()
    assert(fixed.map(_(0).asInstanceOf[Array[Byte]]).exists(p => p(1) == 3))

    val enum = sqlContext.read.avro(testFile).select("enum").collect()
    assert(enum.map(_(0)).toSet == Set("SPADES", "CLUBS", "DIAMONDS"))

    val record = sqlContext.read.avro(testFile).select("record").collect()
    assert(record(0)(0).getClass.toString.contains("Row"))
    assert(record.map(_(0).asInstanceOf[Row](0)).contains("TEST_STR123"))

    val array_of_boolean = sqlContext.read.avro(testFile).select("array_of_boolean").collect()
    assert(array_of_boolean.map(_(0).asInstanceOf[Seq[Boolean]].size).toSet == Set(3, 1, 0))

    val bytes = sqlContext.read.avro(testFile).select("bytes").collect()
    assert(bytes.map(_(0).asInstanceOf[Array[Byte]].length).toSet == Set(3, 1, 0))
  }

  test("sql test") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE avroTable
         |USING com.databricks.spark.avro
         |OPTIONS (path "$episodesFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT * FROM avroTable").collect().length === 8)
  }

  test("conversion to avro and back") {
    // Note that test.avro includes a variety of types, some of which are nullable. We expect to
    // get the same values back.
    TestUtils.withTempDir { dir =>
      val avroDir = s"$dir/avro"
      sqlContext.read.avro(testFile).write.avro(avroDir)
      TestUtils.checkReloadMatchesSaved(sqlContext, testFile, avroDir)
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
      sqlContext.read.avro(testFile).write.options(parameters).avro(avroDir)
      TestUtils.checkReloadMatchesSaved(sqlContext, testFile, avroDir)

      // Look at raw file and make sure has namespace info
      val rawSaved = sqlContext.sparkContext.textFile(avroDir)
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
      val cityRDD = sqlContext.sparkContext.parallelize(Seq(
        Row("San Francisco", 12, new Timestamp(666), null, arrayOfByte),
        Row("Palo Alto", null, new Timestamp(777), null, arrayOfByte),
        Row("Munich", 8, new Timestamp(42), Decimal(3.14), arrayOfByte)))
      val cityDataFrame = sqlContext.createDataFrame(cityRDD, testSchema)

      val avroDir = tempDir + "/avro"
      cityDataFrame.write.avro(avroDir)
      assert(sqlContext.read.avro(avroDir).collect().length == 3)

      // TimesStamps are converted to longs
      val times = sqlContext.read.avro(avroDir).select("Time").collect()
      assert(times.map(_(0)).toSet == Set(666, 777, 42))

      // DecimalType should be converted to string
      val decimals = sqlContext.read.avro(avroDir).select("Decimal").collect()
      assert(decimals.map(_(0)).contains("3.14"))

      // There should be a null entry
      val length = sqlContext.read.avro(avroDir).select("Length").collect()
      assert(length.map(_(0)).contains(null))

      val binary = sqlContext.read.avro(avroDir).select("Binary").collect()
      for (i <- arrayOfByte.indices) {
        assert(binary(1)(0).asInstanceOf[Array[Byte]](i) == arrayOfByte(i))
      }
    }
  }

  test("support of globbed paths") {
    val e1 = sqlContext.read.avro("*/test/resources/episodes.avro").collect()
    assert(e1.length == 8)

    val e2 = sqlContext.read.avro("src/*/*/episodes.avro").collect()
    assert(e2.length == 8)
  }

  test("reading from invalid path throws exception") {

    // Directory given has no avro files
    intercept[FileNotFoundException] {
      TestUtils.withTempDir(dir => sqlContext.read.avro(dir.getCanonicalPath))
    }

    intercept[FileNotFoundException] {
      sqlContext.read.avro("very/invalid/path/123.avro")
    }

    // In case of globbed path that can't be matched to anything, another exception is thrown (and
    // exception message is helpful)
    intercept[FileNotFoundException] {
      sqlContext.read.avro("*/*/*/*/*/*/*/something.avro")
    }

    intercept[FileNotFoundException] {
      TestUtils.withTempDir { dir =>
        FileUtils.touch(new File(dir, "test"))
        sqlContext.read.avro(dir.toString)
      }
    }

  }

  test("SQL test insert overwrite") {
    TestUtils.withTempDir { tempDir =>
      val tempEmptyDir = s"$tempDir/sqlOverwrite"
      // Create a temp directory for table that will be overwritten
      new File(tempEmptyDir).mkdirs()
      sqlContext.sql(
        s"""
           |CREATE TEMPORARY TABLE episodes
           |USING com.databricks.spark.avro
           |OPTIONS (path "$episodesFile")
        """.stripMargin.replaceAll("\n", " "))
      sqlContext.sql(s"""
             |CREATE TEMPORARY TABLE episodesEmpty
             |(name string, air_date string, doctor int)
             |USING com.databricks.spark.avro
             |OPTIONS (path "$tempEmptyDir")
        """.stripMargin.replaceAll("\n", " "))

      assert(sqlContext.sql("SELECT * FROM episodes").collect().length === 8)
      assert(sqlContext.sql("SELECT * FROM episodesEmpty").collect().isEmpty)

      sqlContext.sql(
        s"""
           |INSERT OVERWRITE TABLE episodesEmpty
           |SELECT * FROM episodes
        """.stripMargin.replaceAll("\n", " "))
      assert(sqlContext.sql("SELECT * FROM episodesEmpty").collect().length == 8)
    }
  }

  test("test save and load") {
    // Test if load works as expected
    TestUtils.withTempDir { tempDir =>
      val df = sqlContext.read.avro(episodesFile)
      assert(df.count == 8)

      val tempSaveDir = s"$tempDir/save/"

      df.write.avro(tempSaveDir)
      val newDf = sqlContext.read.avro(tempSaveDir)
      assert(newDf.count == 8)
    }
  }
}