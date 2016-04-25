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
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


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

    val sqlContext = new SQLContext(new SparkContext("local[2]", "AvroReadBenchmark"))

    sqlContext.read.avro(AvroFileGenerator.outputDir).count()

    println("\n\n\nStaring benchmark test - creating DataFrame from benchmark avro files\n\n\n")

    val startTime = System.nanoTime
    sqlContext
      .read
      .avro(AvroFileGenerator.outputDir)
      .select("string")
      .count()
    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\n\n\nFinished benchmark test - result was $executionTime seconds\n\n\n")

    sqlContext.sparkContext.stop()  // Otherwise scary exception message appears
  }
}
