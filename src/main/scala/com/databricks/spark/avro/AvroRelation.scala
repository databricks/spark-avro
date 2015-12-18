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

import java.io.FileNotFoundException
import java.util.zip.Deflater

import scala.collection.JavaConverters._

import com.google.common.base.Objects
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.{DataFileConstants, DataFileReader, FileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.{JobConf, FileInputFormat}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.Logging
import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

private[avro] class AvroRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String])
    (@transient val sqlContext: SQLContext) extends HadoopFsRelation with Logging {

  private val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"
  private val recordName = parameters.getOrElse("recordName", "topLevelRecord")
  private val recordNamespace = parameters.getOrElse("recordNamespace", "")
  private val schemaFromLastPath = parameters.get("schemaFromLastPath")
    .map(_.toBoolean).getOrElse(false)
  private val schemaFromPath = parameters.get("schemaFromPath")

  /** needs to be lazy so it is not evaluated when saving since no schema exists at that location */
  private lazy val avroSchema = {
    val schemaPath = schemaFromPath.getOrElse{
      paths match {
        case Array() =>
          throw new java.io.FileNotFoundException(
            "Cannot infer the schema when no files are present.")
        case array if schemaFromLastPath => array.last
        case array => array.head
      }
    }
    newReader(schemaPath)(_.getSchema)
  }

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
   *
   * @since 1.4.0
   */
  override def dataSchema: StructType = maybeDataSchema.getOrElse(
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType])

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   *
   * Note that the only side effect expected here is mutating `job` via its setters.  Especially,
   * Spark SQL caches [[BaseRelation]] instances for performance, mutating relation internal states
   * may cause unexpected behaviors.
   *
   * @since 1.4.0
   */
  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
    val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, recordNamespace)
    AvroJob.setOutputKeySchema(job, outputAvroSchema)
    val AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
    val AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level"
    val COMPRESS_KEY = "mapred.output.compress"

    sqlContext.getConf(AVRO_COMPRESSION_CODEC, "snappy") match {
      case "uncompressed" =>
        logInfo("writing Avro out uncompressed")
        job.getConfiguration.setBoolean(COMPRESS_KEY, false)
      case "snappy" =>
        logInfo("using snappy for Avro output")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)
      case "deflate" =>
        val deflateLevel = sqlContext.getConf(
          AVRO_DEFLATE_LEVEL, Deflater.DEFAULT_COMPRESSION.toString).toInt
        logInfo(s"using deflate: $deflateLevel for Avro output")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
        job.getConfiguration.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, deflateLevel)
      case unknown: String => logError(s"compression $unknown is not supported")
    }
    new AvroOutputWriterFactory(dataSchema, recordName, recordNamespace)
  }

  /**
   * Filters out unneeded columns before converting into the internal row representation.
   * The first record is used to get the sub-schema that contains only the requested fields,
   * this is then used to generate the field converters and the rows that only
   * contain `requiredColumns`
   */
  override def buildScan(requiredColumns: Array[String], inputs: Array[FileStatus]): RDD[Row] = {
    if (inputs.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val job = new JobConf(sqlContext.sparkContext.hadoopConfiguration)
      val inputAvroSchema = Schema.createRecord(recordName, null, recordNamespace, false)
      inputAvroSchema.setFields(requiredColumns.map{ col =>
        val existing = avroSchema.getField(col)
        val field = new Schema.Field(existing.name, existing.schema, null, existing.defaultValue, 
          existing.order)
        existing.aliases.asScala.foreach(field.addAlias(_))
        field
      }.toList.asJava)
      // i have to do this the ugly way because hadoopFile only exposes one path (why?)
      job.set(org.apache.avro.mapred.AvroJob.INPUT_SCHEMA, inputAvroSchema.toString)
      val fieldConverters = inputAvroSchema.getFields.asScala.map{ f =>
        SchemaConverters.createConverterToSQL(f.schema) }
      FileInputFormat.setInputPaths(job, inputs.map(_.getPath).toArray: _*)

      new HadoopRDD(
        sqlContext.sparkContext,
        job,
        classOf[org.apache.avro.mapred.AvroInputFormat[GenericRecord]],
        classOf[org.apache.avro.mapred.AvroWrapper[GenericRecord]],
        classOf[org.apache.hadoop.io.NullWritable],
        sqlContext.sparkContext.defaultMinPartitions
      ).keys.map{ wrapper => 
        val record = wrapper.datum()
        val row = Row.fromSeq(fieldConverters.zipWithIndex.map{ case (converter, i) => 
          converter(record.get(i)) })
        row
      }
    }
  }

  /**
   * Checks to see if the given Any is the same avro relation based off of the input paths, schema,
   * and partitions
   */
  override def equals(other: Any): Boolean = other match {
    case that: AvroRelation => paths.toSet == that.paths.toSet &&
                                dataSchema == that.dataSchema &&
                                schema == that.schema &&
                                partitionColumns == that.partitionColumns
    case _ => false
  }

  /**
   * Generates a unique has of this relation based off of its paths, schema, and partition
   */
  override def hashCode(): Int = Objects.hashCode(paths.toSet, dataSchema, schema, partitionColumns)

  /**
   * Opens up the location to for reading. Takes in a function to run on the schema and returns the
   * result of this function. This takes in a function so that the caller does not have to worry
   * about cleaning up and closing the reader.
   * @param location the location in the filesystem to read from
   * @param fun the function that is called on when the reader has been initialized
   * @tparam T the return type of the function given
   */
  private def newReader[T](location: String)(fun: FileReader[GenericRecord] => T): T = {
    val path = new Path(location)
    val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(path.toUri, hadoopConfiguration)

    val statuses = fs.globStatus(path) match {
      case null => throw new FileNotFoundException(s"The path ($location) is invalid.")
      case globStatus => globStatus.toStream.map(_.getPath).flatMap(getAllFiles(fs, _))
    }

    val singleFile =
      (if (hadoopConfiguration.getBoolean(IgnoreFilesWithoutExtensionProperty, true)) {
        statuses.find(_.getName.endsWith("avro"))
      } else {
        statuses.headOption
      }).getOrElse(throw new FileNotFoundException(s"No avro files present at ${path.toString}"))

    val reader = DataFileReader.openReader(new FsInput(singleFile, hadoopConfiguration),
      new GenericDatumReader[GenericRecord]())
    val result = fun(reader)
    reader.close()
    result
  }

  private def getAllFiles(fs: FileSystem, path: Path): Stream[Path] = {
    if (fs.isDirectory(path)) {
      fs.listStatus(path).toStream.map(_.getPath).flatMap(getAllFiles(fs, _))
    } else {
      Stream(path)
    }
  }
}
