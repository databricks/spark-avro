# Spark SQL Avro Library

A library for querying Avro data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Requirements

This library requires Spark 1.2+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.databricks.spark
artifactId: spark-avro_2.10
version: 0.1
```
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

### Scala API

You can use the library by loading the implicits from `com.databricks.spark.avro._`.

```
scala> import org.apache.spark.sql.SQLContext

scala> val sqlContext = new SQLContext(sc)

scala> import com.databricks.spark.avro._

scala> val episodes = sqlContext.avroFile("episodes.avro")
episodes: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[0] at RDD at SchemaRDD.scala:104
== Query Plan ==
== Physical Plan ==
PhysicalRDD [title#0,air_date#1,doctor#2], MappedRDD[2] at map at AvroRelation.scala:54

scala> import sqlContext._
import sqlContext._

scala> episodes.select('title').collect()
res0: Array[org.apache.spark.sql.Row] = Array([The Eleventh Hour], [The Doctor's Wife], [Horror of Fang Rock], [An Unearthly Child], [The Mysterious Planet], [Rose], [The Power of the Daleks], [Castrolava])
```

### SQL API
Avro data can be queried in pure SQL by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE episodes
USING com.databricks.spark.avro
OPTIONS (path "episodes.avro")
```

### Java API
Avro files can be read using static functions in AvroUtils.

```java
import com.databricks.spark.avro.AvroUtils;

JavaSchemaRDD episodes = AvroUtils.avroFile(sqlContext, "episodes.avro");
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script.  To build a JAR file simply run `sbt/sbt package` from the project root.

