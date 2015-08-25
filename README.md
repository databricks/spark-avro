# Spark SQL Avro Library

A library for querying Avro data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) and saving Spark SQL as Avro.

[![Build Status](https://travis-ci.org/databricks/spark-avro.svg?branch=master)](https://travis-ci.org/databricks/spark-avro)
[![codecov.io](http://codecov.io/github/databricks/spark-avro/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-avro?branch=master)

## Requirements

This library requires Spark 1.3+. There is a 1.2 version as well.

## Linking
You can link against this library (for Spark 1.3+) in your program at the following coordinates:

```
groupId: com.databricks
artifactId: spark-avro_2.10
version: 1.0.0
```

Using SBT: `libraryDependencies += "com.databricks" %% "spark-avro" % "1.0.0"`

<!---
TODO: Add a link to download the JAR directly for e.g. adding to the Spark shell
--->

The spark-avro jar file can also be added to a Spark using the `--jars` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-avro_2.10-1.0.0.jar
```

For use with Spark 1.2, you can use version `0.2.0` instead.

## Features
These examples use an avro file available for download
[here](https://github.com/databricks/spark-avro/raw/master/src/test/resources/episodes.avro):

```
$ wget https://github.com/databricks/spark-avro/raw/master/src/test/resources/episodes.avro
```

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

### Scala API

A recommended way to read query Avro data in sparkSQL, or save sparkSQL data as Avro is by using
native DataFrame APIs (available in Scala, Java and Python, starting from Spark 1.3):

```scala
import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
// Creates a DataFrame from a specified file
val df = sqlContext.load("src/test/resources/episodes.avro", "com.databricks.spark.avro")
// Saves df to /new/dir/ as avro files
df.save("/new/dir/", "com.databricks.spark.avro")
```

An alternative way is to use `avroFile` and `save` methods (available in 1.2+):

```scala
scala> import org.apache.spark.sql.SQLContext

scala> val sqlContext = new SQLContext(sc)

scala> import com.databricks.spark.avro._

scala> val episodes = sqlContext.avroFile("episodes.avro", 20)
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

`avroFile` allows you to specify the `minPartitions` parameter as its second argument.
To save DataFrame as avro you should use the `save` method in `AvroSaver`. For example:

```scala
scala> AvroSaver.save(myRDD, "my/output/dir")
```

Avro schema's field 'doc' will be preserved in DataFrame's schema metadata.

To include aliases column in scheme invoke
```
val df = sqlContext.read.format("com.databricks.spark.avro").option("withAlias", "true").load("test.avro")
```
In Spark SQL, loading avro with alias columns use:
```
create table mytable using com.databricks.spark.avro OPTIONS(path "test.avro", withAlias "true");
```
While saving, alias columns in DataFrame schema will be stored in `aliases` of avro file.

You can also specifiy the the record name and namespace with optional parameters:
```scala
scala> AvroSaver.save(myRDD, "my/output/dir", Map("recordName" -> "MyRecord", "recordNamespace" -> "com.mycompany.mystuff"))
```

We also support the ability to read all avro files from some directory. To do that, you can pass
a path to that directory to the avroFile() function. However, there is a limitation - all of
those files must have the same schema. Additionally, files used must have a .avro extension.


### Java API

The recommended way to query avro is to use the native DataFrame APIs.
The code is almost identical to Scala:

```java
import org.apache.spark.sql.*;
SQLContext sqlContext = new SQLContext(sc);
// Creates a DataFrame from a specified file
DataFrame df = sqlContext.load("src/test/resources/episodes.avro", "com.databricks.spark.avro");
// Saves df to /new/dir/ as avro files
df.save("/new/dir/", "com.databricks.spark.avro");
```


### Python API

As mentioned before, a recommended way to query avro is to use native DataFrame APIs.
The code is almost identical to Scala:

```python
# Creates a DataFrame from a specified file
df = sqlContext.load("src/test/resources/episodes.avro", "com.databricks.spark.avro")
# Saves df to /new/dir/ as avro files
df.save("/new/dir/", "com.databricks.spark.avro")
```

### SQL API
Avro data can be queried in pure SQL by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE episodes
USING com.databricks.spark.avro
OPTIONS (path "src/test/resources/episodes.avro")
```

### Versions
Spark changed how it reads / writes data in 1.4, so please use the correct version
of this dedicated for your spark version

1.3 -> 1.0.0

1.4+ -> 1.1.0-SNAPSHOT

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
