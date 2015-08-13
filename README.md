# Spark SQL Avro Library

A library for querying Avro data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) and saving Spark SQL as Avro.

[![Build Status](https://travis-ci.org/databricks/spark-avro.svg?branch=master)](https://travis-ci.org/databricks/spark-avro)
[![codecov.io](http://codecov.io/github/databricks/spark-avro/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-avro?branch=master)

## Requirements

This documentation is for Spark 1.4+.

This library has different versions for 1.2, 1.3, and 1.4.

### Versions
Spark changed how it reads / writes data in 1.4, so please use the correct version
of this dedicated for your spark version

1.2  -> `0.2.0`

1.3 -> `1.0.0`

1.4+ -> `1.1.0`

## Linking
You can link against this library (for Spark 1.3+) in your program at the following coordinates:

Using SBT: `libraryDependenicies += "com.databricks" %% "spark-avro_2.10" % "1.1.0"`

using Maven: 
```xml
<dependency>
    <groupId>com.databricks<groupId>
    <artifactId>spark avro_2.10</artifactId>
    <version>1.1.0</version>
</dependency>
```

<!---
TODO: Add a link to download the JAR directly for e.g. adding to the Spark shell
--->

The spark-avro jar file can also be added to a Spark using the `--jars` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-avro_2.10-1.0.0.jar
```

## Features

Spark-Avro supports most conversions between Spark-SQL and Avro records, making
Avro a first-class citizen in Spark. This library will automatically do all the
required schema conversions for you.

### Partitioning

This library allows developers to easily read and write partitioned data
witout any extra configuration. Just pass the columns you want to
partition on just like you would for parquet.


### Compression

You can specify the type of compression to use when writing Avro out to
disk. The supported types are **uncompressed**, **snappy**, and **deflate**.
You can also specify the deflate level.

### Specifying record name

You can specify the record name and namespace to use by passing the a map
of parameters with **recordName** and **recordNamespace**.

## Supported types for Avro -> SparkSQL conversion
As of now, every avro type with the exception of complex unions is supported. To be more specific,
we use the following mapping from avro types to SparkSQL types:

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

3) union(something, null), where something is one of the avro types mentioned above, including
two types of unions.

At the moment we ignore docs, aliases and other properties present in the avro file.

## Supported types for SparkSQL -> Avro conversion

Every SparkSQL type is supported. For most of them the corresponding type is obvious
(e.g. IntegerType gets converted to int), for the rest the following conversions are used:

```
ByteType -> int
ShortType -> int
DecimalType -> string
BinaryType -> bytes
TimestampType -> long
StructType -> record
```

## Examples

These examples use an avro file available for download
[here](https://github.com/databricks/spark-avro/raw/master/src/test/resources/episodes.avro):

### Scala API

A recommended way to read query Avro data in sparkSQL, or save sparkSQL data as Avro is by using
native DataFrame APIs (available in Scala, Java and Python, starting from Spark 1.3):

```scala
// import needed for the .avro method to be added
import com.databricks.spark.avro._
		
val sqlContext = new SQLContext(sc)

// The Avro records get converted to spark types, filtered, and
// then written back out as Avro records
val df = sqlContext.read.avro("src/test/resources/episodes.avro")
df.filter("doctor > 5").write.avro("/tmp/output")
```

Alternativly you can specify the format to use instead:

```scala
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
	.format("com.databricks.spark.avro")
	.load("src/test/resources/episodes.avro")
	
df.filter("doctor > 5").write
	.format("com.databricks.spark.avro")
	.save("/tmp/output")
```

You can specify the compression like this:

```scala
import com.databricks.spark.avro._
val sqlContext = new SQLContext(sc)

// configuration to use deflate compression
sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")
sqlContext.setConf("spark.sql.avro.deflate.level", "5")

val df = sqlContext.read.avro("src/test/resources/episodes.avro")

// writes out compressed Avro records
df.write.avro("/tmp/output")
```

You can write partitioned Avro records like this:

```scala
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

val df = Seq((2012, 8, "Batman", 9.8),
	(2012, 8, "Hero", 8.7),
	(2012, 7, "Robot", 5.5),
	(2011, 7, "Git", 2.0))
	.toDF("year", "month", "title", "rating")

df.write.partitionBy("year", "month").avro("/tmp/output")
```

You can specify the record name and namespace like this:

```scala
import com.databricks.spark.avro._

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.avro("src/test/resources/episodes.avro")

val name = "AvroTest"
val namespace = "com.databricks.spark.avro"
val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)

df.write.options(parameters).avro("/tmp/output")
```

### Java API

The recommended way to query avro is to use the native DataFrame APIs.
The code is almost identical to Scala:

```java
import org.apache.spark.sql.*;

SQLContext sqlContext = new SQLContext(sc);

// Creates a DataFrame from a specified file
DataFrame df = sqlContext.read().format("com.databricks.spark.avro")
    .load("src/test/resources/episodes.avro");

// Saves the subset of the Avro records read in
df.filter($"age > 5").write()
	.format("com.databricks.spark.avro")
	.save("/tmp/output");
```


### Python API

As mentioned before, a recommended way to query avro is to use native DataFrame APIs.
The code is almost identical to Scala:

```python
# Creates a DataFrame from a specified directory
df = sqlContext.read.format("com.databricks.spark.avro").load("src/test/resources/episodes.avro")

#  Saves the subset of the Avro records read in
subset = df.where("age > 5")
subset.write.format("com.databricks.spark.avro").save("output")
```

### SQL API
Avro data can be queried in pure SQL by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE episodes
USING com.databricks.spark.avro
OPTIONS (path "src/test/resources/episodes.avro")
```



## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html),
which is automatically downloaded by the included shell script.  To build a JAR file simply run
`sbt/sbt package` from the project root.

## Testing
To run the tests, you should run `sbt/sbt test`. In case you are doing improvements that target
speed, you can generate a sample avro file and check how long does it take to read that avro file
using the following commands:

`sbt/sbt "test:run-main com.databricks.spark.avro.AvroFileGenerator NUMBER_OF_RECORDS NUMBER_OF_FILES"`
will create sample avro files in `target/avroForBenchmark/`. You can specify the number of records
for each file, as well as the overall number of files.

`sbt/sbt "test:run-main com.databricks.spark.avro.AvroReadBenchmark"` runs `count()` on the data
inside `target/avroForBenchmark/` and tells you how long did the operation take.

Similarly, you can do benchmarks on how long does it take to write DataFrame as avro file with:

`sbt/sbt "test:run-main com.databricks.spark.avro.AvroWriteBenchmark NUMBER_OF_ROWS"`, where
`NUMBER_OF_ROWS` is an optional parameter that allows you to specify the number of rows in
DataFrame that we will be writing.
