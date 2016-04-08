package com.databricks.spark.avro

import java.sql.Date
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.util.Random
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

/**
 * This object runs a simple benchmark test to find out how long does it take to write a large
 * DataFrame to an avro file. It reads one argument, which specifies how many rows does the
 * DataFrame that we're writing contain.
 */
object AvroWriteBenchmark {

  val defaultNumberOfRows = 1000000
  val defaultSize = 100 // Size used for items in generated RDD like strings, arrays and maps

  val testSchema = StructType(Seq(
    StructField("StringField", StringType, false),
    StructField("IntField", IntegerType, true),
    StructField("dateField", DateType, true),
    StructField("DoubleField", DoubleType, false),
    StructField("DecimalField", DecimalType(10, 10), true),
    StructField("ArrayField", ArrayType(BooleanType), false),
    StructField("MapField", MapType(StringType, IntegerType), true),
    StructField("StructField", StructType(Seq(StructField("id", IntegerType, true))), false)))

  private def generateRandomRow(): Row = {
    val rand = new Random()
    Row(rand.nextString(defaultSize), rand.nextInt(), new Date(rand.nextLong()) ,rand.nextDouble(), rand.nextDouble(),
      TestUtils.generateRandomArray(rand, defaultSize).toSeq,
      TestUtils.generateRandomMap(rand, defaultSize).toMap, Row(rand.nextInt()))
  }

  def main(args: Array[String]) {
    var numberOfRows = defaultNumberOfRows
    if (args.size > 0) {
      numberOfRows = args(0).toInt
    }

    println(s"\n\n\nPreparing for a benchmark test - creating a RDD with $numberOfRows rows\n\n\n")

    val sqlContext = new SQLContext(new SparkContext("local[2]", "AvroReadBenchmark"))

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    val testDataFrame = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(0 until numberOfRows).map(_ => generateRandomRow()),
      testSchema)

    println("\n\n\nStaring benchmark test - writing a DataFrame as avro file\n\n\n")

    val startTime = System.nanoTime

    testDataFrame.write.avro(avroDir)

    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    FileUtils.deleteDirectory(tempDir)
    sqlContext.sparkContext.stop()  // Otherwise scary exception message appears
  }
}
