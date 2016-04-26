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

import java.io.{FileNotFoundException, IOException, ObjectInputStream, ObjectOutputStream}
import java.net.URI
import java.util.zip.Deflater

import scala.util.control.NonFatal

import com.databricks.spark.avro.DefaultSource.{IgnoreFilesWithoutExtensionProperty, SerializableConfiguration}
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.{DataFileConstants, DataFileReader}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericRow, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}

private[avro] class DefaultSource extends FileFormat with DataSourceRegister with Logging {
  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = sqlContext.sparkContext.hadoopConfiguration

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

    val avroSchema = {
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
      sqlContext: SQLContext,
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

    sqlContext.getConf(AVRO_COMPRESSION_CODEC, "snappy") match {
      case "uncompressed" =>
        logInfo("writing uncompressed Avro records")
        job.getConfiguration.setBoolean(COMPRESS_KEY, false)

      case "snappy" =>
        logInfo("compressing Avro output using Snappy")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)

      case "deflate" =>
        val deflateLevel = sqlContext.getConf(
          AVRO_DEFLATE_LEVEL, Deflater.DEFAULT_COMPRESSION.toString).toInt
        logInfo(s"compressing Avro output using deflate (level=$deflateLevel)")
        job.getConfiguration.setBoolean(COMPRESS_KEY, true)
        job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
        job.getConfiguration.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, deflateLevel)

      case unknown: String =>
        logError(s"unsupported compression codec $unknown")
    }

    new AvroOutputWriterFactory(dataSchema, recordName, recordNamespace)
  }

  override def buildReader(
      sqlContext: SQLContext,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String]): (PartitionedFile) => Iterator[InternalRow] = {

    val conf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val broadcastedConf = sqlContext.sparkContext.broadcast(new SerializableConfiguration(conf))

    (file: PartitionedFile) => {
      val reader = {
        val conf = broadcastedConf.value.value
        val in = new FsInput(new Path(new URI(file.filePath)), conf)
        DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]())
      }

      val fieldExtractors = {
        val avroSchema = reader.getSchema
        requiredSchema.zipWithIndex.map { case (field, index) =>
          val avroField = Option(avroSchema.getField(field.name)).getOrElse {
            throw new IllegalArgumentException(
              s"""Cannot find required column ${field.name} in Avro schema:"
                 |
                 |${avroSchema.toString(true)}
               """.stripMargin
            )
          }

          val converter = SchemaConverters.createConverterToSQL(avroField.schema())

          (record: GenericRecord, buffer: Array[Any]) => {
            buffer(index) = converter(record.get(avroField.pos()))
          }
        }
      }

      new Iterator[InternalRow] {
        private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

        private val safeRow = new GenericRow(rowBuffer)

        // Used to convert `Row`s containing data columns into `InternalRow`s.
        private val encoderForDataColumns = RowEncoder(requiredSchema)

        private val unsafePartitionValues = {
          val partitionColumns = toAttributes(partitionSchema)
          val toUnsafe = GenerateUnsafeProjection.generate(partitionColumns, partitionColumns)
          toUnsafe(file.partitionValues)
        }

        private val unsafeRowJoiner =
          GenerateUnsafeRowJoiner.generate((requiredSchema, partitionSchema))

        private def toAttributes(schema: Seq[StructField]): Seq[Attribute] = schema.map { f =>
          AttributeReference(f.name, f.dataType, f.nullable)()
        }

        override def hasNext: Boolean = reader.hasNext

        override def next(): InternalRow = {
          val record = reader.next()

          var i = 0
          while (i < requiredSchema.length) {
            fieldExtractors(i)(record, rowBuffer)
            i += 1
          }

          val unsafeDataRow = encoderForDataColumns.toRow(safeRow).asInstanceOf[UnsafeRow]
          unsafeRowJoiner.join(unsafeDataRow, unsafePartitionValues)
        }
      }
    }
  }
}

private[avro] object DefaultSource {
  val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"

  class SerializableConfiguration(@transient var value: Configuration)
    extends Serializable with Logging {

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
          logError("Exception encountered", e)
          throw e
        case NonFatal(e) =>
          logError("Exception encountered", e)
          throw new IOException(e)
      }
    }
  }
}
