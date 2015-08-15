package com.databricks.spark.avro

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.test.TestSQLContext


/**
 * This object runs a simple benchmark test on the avro files in benchmarkFilesDir. It measures
 * how long does it take to convert them into DataFrame and run count() method on them. See
 * README on how to invoke it.
 */
object AvroReadBenchmark {

  def main(args: Array[String]) {
    val benchmarkDirFiles = new File(AvroFileGenerator.outputDir).list
    if (benchmarkDirFiles == null || benchmarkDirFiles.isEmpty) {
      sys.error(s"The benchmark directory ($AvroFileGenerator.outputDir) does not exist or " +
        "is empty. First you should generate some files to run a benchmark with (see README)")
    }

    TestSQLContext.read.avro(AvroFileGenerator.outputDir).count()

   println("\n\n\nStaring benchmark test - creating DataFrame from benchmark avro files\n\n\n")

    val startTime = System.nanoTime
    TestSQLContext
      .read
      .avro(AvroFileGenerator.outputDir)
      .select("string")
      .count()
    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    TestSQLContext.sparkContext.stop()  // Otherwise scary exception message appears
  }
}
