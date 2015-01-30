package com.databricks.spark.avro

import java.io.File

import org.apache.spark.sql.test.TestSQLContext


/**
 * This object runs a simple benchmark test on the avro files in benchmarkFilesDir. It measures
 * how long does it take to convert them into SchemaRDD and run count() method on them. See
 * README on how to invoke it.
 */
object AvroReadBenchmark {

  val benchmarkFilesDir = "src/test/resources/avroForBenchmark/"

  def main(args: Array[String]) {
    if (new File(benchmarkFilesDir).list.isEmpty) {
      sys.error("First you should generate some files to run a benchmark with (see README)")
    }

    println("\n\n\nStaring benchmark test - creating SchemaRDD from benchmark avro files\n\n\n")

    val startTime = System.currentTimeMillis
    TestSQLContext
      .avroFile(benchmarkFilesDir)
      .count()
    val endTime = System.currentTimeMillis
    val executionTime = (endTime - startTime) / 1000

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    TestSQLContext.sparkContext.stop // Otherwise scary exception message appears
  }
}
