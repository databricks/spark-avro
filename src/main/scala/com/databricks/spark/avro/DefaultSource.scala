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

import java.io._
import java.net.URI
import java.util.zip.Deflater

import scala.util.control.NonFatal

import com.databricks.spark.avro.DefaultSource.{AvroSchema, IgnoreFilesWithoutExtensionProperty, SerializableConfiguration}
import com.esotericsoftware.kryo.DefaultSerializer
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.file.{DataFileConstants, DataFileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

private[avro] class DefaultSource extends FileFormat with DataSourceRegister {
  private val log = LoggerFactory.getLogger(getClass)

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
      spark: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sparkContext.hadoopConfiguration

    // Schema evolution is not supported yet. Here we only pick a single random sample file to
    // figure out the schema of the whole dataset.
    val sampleFile = if (conf.getBoolean(IgnoreFilesWithoutExtensionProperty, true)) {
      files.find(_.getPath.getName.endsWith(".avro")).getOrElse {
        throw new FileNotFoundException(
          "No Avro files found. Hadoop option \"avro.mapred.ignore.inputs.without.extension\" is " +
            "set to true. Do all input files have \".avro\" extension?"
        )
      }
    } else {
      files.headOption.getOrElse {
        throw new FileNotFoundException("No Avro files found.")
      }
    }

    // User can specify an optional avro json schema.
    val avroSchema = options.get(AvroSchema).map(new Schema.Parser().parse).getOrElse {
      val in = new FsInput(sampleFile.getPath, conf)
      val reader = DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]())
      reader.getSchema
    }

    SchemaConverters.toSqlType(avroSchema).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${avroSchema.toString(true)}
           |""".stripMargin)
    }
  }

  override def shortName(): String = "avro"

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val recordName = options.getOrElse("recordName", "topLevelRecord")
    val recordNamespace = options.getOrElse("recordNamespace", "")
    val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
    val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, recordNamespace)

    AvroJob.setOutputKeySchema(job, outputAvroSchema)
    val AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec"
    val AVRO_DEFLATE_LEVEL = "spark.sql.avro.deflate.level"
    val COMPRESS_KEY = "mapred.output.compress"

    spark.conf.get(AVRO_COMPRESSION_CODEC, "snappy") match {
      case "uncompressed" =>
        log.info("writing uncompressed Avro records")
        job.getConfiguration.setBoolean(COMPRESS_KEY, false)

      case "snappy" =>
        log.info("compressing Avro output using Snappy")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)

      case "deflate" =>
        val deflateLevel = spark.conf.get(
          AVRO_DEFLATE_LEVEL, Deflater.DEFAULT_COMPRESSION.toString).toInt
        log.info(s"compressing Avro output using deflate (level=$deflateLevel)")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
        job.getConfiguration.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, deflateLevel)

      case unknown: String =>
        log.error(s"unsupported compression codec $unknown")
    }

    new AvroOutputWriterFactory(dataSchema, recordName, recordNamespace)
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value

      // TODO Removes this check once `FileFormat` gets a general file filtering interface method.
      // Doing input file filtering is improper because we may generate empty tasks that process no
      // input files but stress the scheduler. We should probably add a more general input file
      // filtering mechanism for `FileFormat` data sources. See SPARK-16317.
      if (
        conf.getBoolean(IgnoreFilesWithoutExtensionProperty, true) &&
        !file.filePath.endsWith(".avro")
      ) {
        Iterator.empty
      } else {
        val reader = {
          val in = new FsInput(new Path(new URI(file.filePath)), conf)
          DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]())
        }

        val rowConverter = SchemaConverters.createConverterToSQL(reader.getSchema, requiredSchema)


        new Iterator[InternalRow] {
          // Used to convert `Row`s containing data columns into `InternalRow`s.
          private val encoderForDataColumns = RowEncoder(requiredSchema)

          override def hasNext: Boolean = reader.hasNext

          override def next(): InternalRow = {
            val record = reader.next()
            val safeDataRow = rowConverter(record).asInstanceOf[GenericRow]

            // The safeDataRow is reused, we must do a copy
            encoderForDataColumns.toRow(safeDataRow)
          }
        }
      }
    }
  }
}

private[avro] object DefaultSource {
  val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"

  val AvroSchema = "avroSchema"

  @DefaultSerializer(classOf[KryoJavaSerializer])
  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    @transient private[avro] lazy val log = LoggerFactory.getLogger(getClass)

    private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
      out.defaultWriteObject()
      value.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
      value = new Configuration(false)
      value.readFields(in)
    }

    private def tryOrIOException[T](block: => T): T = {
      try {
        block
      } catch {
        case e: IOException =>
          log.error("Exception encountered", e)
          throw e
        case NonFatal(e) =>
          log.error("Exception encountered", e)
          throw new IOException(e)
      }
    }
  }
}
