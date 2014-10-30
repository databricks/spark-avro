# Spark SQL Avro Library

A library for querying Avro data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Building
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script.  To build a JAR file simply run `sbt/sbt package` from the project root.

## Using with Spark
This library requires Spark 1.2+.

The jar file produced above can be added to a spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
spark $ bin/spark-shell --jars ../sql-avro/target/scala-2.10/sql-avro_2.10-0.1.jar

Spark assembly has been built with Hive, including Datanucleus jars on classpath
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.2.0-SNAPSHOT
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45)
Type in expressions to have them evaluated.
Type :help for more information.
2014-10-30 13:21:11.442 java[4635:1903] Unable to load realm info from SCDynamicStore
Spark context available as sc.
```

### Scala API

You can use the library by loading the implicits from `org.apache.spark.sql.avro`.

```
scala> import org.apache.spark.sql.avro._
import org.apache.spark.sql.avro._

scala> val episodes = sqlContext.avroFile("../sqlAvro/src/test/resources/episodes.avro")
episodes: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[0] at RDD at SchemaRDD.scala:104
== Query Plan ==
== Physical Plan ==
PhysicalRDD [title#0,air_date#1,doctor#2], MappedRDD[2] at map at AvroRelation.scala:54

scala> import sqlContext._
import sqlContext._

scala> episodes.select('title).collect()
res0: Array[org.apache.spark.sql.Row] = Array([The Eleventh Hour], [The Doctor's Wife], [Horror of Fang Rock], [An Unearthly Child], [The Mysterious Planet], [Rose], [The Power of the Daleks], [Castrolava])
```

### SQL API
Avro data can be queried in pure SQL by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE episodes
USING org.apache.spark.sql.avro
OPTIONS (path "../sql-avro/src/test/resources/episodes.avro")
```

### Java API
Avro files can be read using static functions in AvroUtils.

```java
import org.apache.spark.sql.avro.AvroUtils;

JavaSchemaRDD episodes = AvroUtils.loadAvroFile(sqlContext, "../sql-avro/src/test/resources/episodes.avro");
```
