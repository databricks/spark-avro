# Spark SQL Avro Library

A library for querying Avro data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) and saving Spark SQL as Avro.

[![Build Status](https://travis-ci.org/databricks/spark-avro.svg?branch=master)](https://travis-ci.org/databricks/spark-avro)

## Requirements

This library requires Spark 1.2+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.databricks
artifactId: spark-avro_2.10
version: 0.1
```

Using SBT: `libraryDependencies += "com.databricks" %% "spark-avro" % "0.1"`

<!---
TODO: Add a link to download the JAR directly for e.g. adding to the Spark shell
--->

The spark-avro jar file can also be added to a Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-avro_2.10-0.1.jar

Spark assembly has been built with Hive, including Datanucleus jars on classpath
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.2.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45)
Type in expressions to have them evaluated.
Type :help for more information.
2014-10-30 13:21:11.442 java[4635:1903] Unable to load realm info from SCDynamicStore
Spark context available as sc.
```

## Features
These examples use an avro file available for download [here](https://github.com/databricks/spark-avro/raw/master/src/test/resources/episodes.avro):

```
$ wget https://github.com/databricks/spark-avro/raw/master/src/test/resources/episodes.avro
```

## Supported types for Avro -> SparkSQL conversion
As of now, every avro type with the exception of complex unions is supported. To be more specific, we use the following mapping from avro types to SparkSQL types:

```
boolean -> BooleanType
int -> IntegerType
long -> LongType
float -> FloatType
double -> DoubleType
bytes -> BinaryType
string -> StringType
record -> StructType
enum -> StringType
array -> ArrayType
map -> MapType
fixed -> BinaryType
```

As for unions, we only support three kinds of unions:

1) union(int, long)

2) union(float, double)

3) union(something, null), where something is one of the avro types mentioned above, including two types of unions.

At the moment we ignore docs, aliases and other properties present in the avro file.

## Supported types for SparkSQL -> Avro conversion

Every SparkSQL type is supported. For most of them the corresponding type is obvious (e.g. IntegerType gets converted to int), for the rest the following conversions are used:

```
ByteType -> int
ShortType -> int
DecimalType -> string
BinaryType -> bytes
TimestampType -> long
StructType -> record
```

### Scala API

To get a DataFrame from avro file, you can use the library by loading the implicits from `com.databricks.spark.avro._`.

```
scala> import org.apache.spark.sql.SQLContext

scala> val sqlContext = new SQLContext(sc)

scala> import com.databricks.spark.avro._

scala> val episodes = sqlContext.avroFile("episodes.avro")
episodes: org.apache.spark.sql.DataFrame =
DataFrame[0] at RDD at DataFrame.scala:104
== Query Plan ==
== Physical Plan ==
PhysicalRDD [title#0,air_date#1,doctor#2], MappedRDD[2] at map at AvroRelation.scala:54

scala> import sqlContext._
import sqlContext._

scala> episodes.select('title).collect()
res0: Array[org.apache.spark.sql.Row] = Array([The Eleventh Hour], [The Doctor's Wife], [Horror of Fang Rock], [An Unearthly Child], [The Mysterious Planet], [Rose], [The Power of the Daleks], [Castrolava])
```

To save DataFrame as avro you should use the `save()` method in `AvroSaver`. For example:

```
scala> AvroSaver.save(myRDD, "my/output/dir")
```

We also support the ability to read all avro files from some directory. To do that, you can pass a path to that directory to the avroFile() function. However, there is a limitation - all of those files must have the same schema.

### Python and SQL API
Avro data can be queried in pure SQL or from python by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE episodes
USING com.databricks.spark.avro
OPTIONS (path "episodes.avro")
```

### Java API
Avro files can be read using static functions in AvroUtils. Same goes for saving DataFrame as Avro.

```java
import com.databricks.spark.avro.AvroUtils;

JavaDataFrame episodes = AvroUtils.avroFile(sqlContext, "episodes.avro");
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script.  To build a JAR file simply run `sbt/sbt package` from the project root.

## Testing
To run the tests, you should run `sbt/sbt test`. In case you are doing improvements that target speed, you can generate a sample avro file and check how long does it take to read that avro file using the following commands:

`sbt/sbt "test:run-main com.databricks.spark.avro.AvroFileGenerator NUMBER_OF_RECORDS NUMBER_OF_FILES"` will create sample avro files in `target/avroForBenchmark/`. You can specify the number of records for each file, as well as the overall number of files.

`sbt/sbt "test:run-main com.databricks.spark.avro.AvroReadBenchmark"` runs `count()` on the data inside `target/avroForBenchmark/` and tells you how long did the operation take.

Similarly, you can do benchmarks on how long does it take to write DataFrame as avro file with:

 `sbt/sbt "test:run-main com.databricks.spark.avro.AvroWriteBenchmark NUMBER_OF_ROWS"`, where `NUMBER_OF_ROWS` is an optional parameter that allows you to specify the number of rows in DataFrame that we will be writing.
