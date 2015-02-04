package com.databricks.spark.avro

import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.util.Random

import com.google.common.io.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._


/**
 * This object runs a simple benchmark test to find out how long does it take to write a large
 * SchemaRDD to an avro file. It reads one argument, which specifies how many rows does the
 * SchemaRDD that we're writing contain.
 */
object AvroWriteBenchmark {

  val defaultNumberOfRows = 50000
  val defaultSize = 100 // Size used for items in generated RDD like strings, arrays and maps

  val testSchema = StructType(Seq(
    StructField("StringField", StringType, false),
    StructField("IntField", IntegerType, true),
    StructField("DoubleField", DoubleType, false),
    StructField("DecimalField", DecimalType(10, 10), true),
    StructField("ArrayField", ArrayType(BooleanType), false),
    StructField("MapField", MapType(StringType, IntegerType), true),
    StructField("StructField", StructType(Seq(StructField("id", IntegerType, true))), false)))

  private[avro] def createLargeRDD(numberOfRows: Int): RDD[Row] = {
    val rows = new Array[Row](numberOfRows)
    val rand = new Random()

    for (i <- 0 until numberOfRows) {
      rows(i) = Row(rand.nextString(defaultSize), rand.nextInt, rand.nextDouble, rand.nextDouble,
        AvroFileGenerator.generateRandomArray(rand, defaultSize).toSeq,
        AvroFileGenerator.generateRandomMap(rand, defaultSize).toMap, Row(rand.nextInt))
    }

    sparkContext.parallelize(rows)
  }

  def main(args: Array[String]) {
    var numberOfRows = defaultNumberOfRows
    if (args.size > 0) {
      numberOfRows = args(0).toInt
    }

    println(s"\n\n\nPreparing for a benchmark test - creating a RDD with $numberOfRows rows\n\n\n")

    val tempDir = Files.createTempDir()
    val avroDir = tempDir + "/avro"
    val testSchemaRDD = TestSQLContext.applySchema(createLargeRDD(numberOfRows), testSchema)

    println("\n\n\nStaring benchmark test - writing a SchemaRDD as avro file\n\n\n")

    val startTime = System.nanoTime

    AvroSaver.save(testSchemaRDD, avroDir)

    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    DirectoryDeletion.recursiveDelete(tempDir)
    TestSQLContext.sparkContext.stop // Otherwise scary exception message appears
  }
}
